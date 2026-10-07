package com.campus.scheduler.core;

import com.campus.scheduler.config.SchedulerProperties;
import com.campus.scheduler.model.ExecutorNode;
import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.model.JobInstance;
import com.campus.scheduler.repo.ExecutorRepo;
import com.campus.scheduler.repo.JobInfoRepo;
import com.campus.scheduler.repo.JobInstanceRepo;
import com.campus.scheduler.repo.JobLogRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 故障转移 + misfire 补偿。
 *
 * <p>调度循环负责「正常路径」，这里负责「异常路径」。分两个类是因为
 * 它们的触发频率差很多（1 秒 vs 30 秒），而且职责不同。
 *
 * <h2>它解决三种异常</h2>
 * <ol>
 *   <li><b>执行器宕机</b>：心跳超时 → 标记离线 → 它身上没跑完的任务重新路由</li>
 *   <li><b>派发时执行器恰好挂了</b>：实例留在 PENDING 且 executor_id 指向已离线节点 → 同上</li>
 *   <li><b>调度中心停机</b>：重启后有一批「触发时刻早就过去但没人执行」的实例 → misfire 补偿</li>
 * </ol>
 *
 * <p>这三种异常的共同点是：<b>不重跑已经成功/失败的任务</b>。
 * 所以所有查询都只挑 PENDING / RUNNING 状态，这是不重复执行的又一道保障。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FailoverTask {

    private final SchedulerProperties props;
    private final ExecutorRepo executorRepo;
    private final JobInstanceRepo instanceRepo;
    private final JobInfoRepo jobRepo;
    private final JobLogRepo logRepo;
    private final InstanceDispatcher instanceDispatcher;

    public final AtomicLong statOfflineMarked = new AtomicLong();
    public final AtomicLong statTransferred   = new AtomicLong();
    public final AtomicLong statMisfireFixed  = new AtomicLong();
    public final AtomicLong statMisfireSkipped = new AtomicLong();
    public final AtomicLong statRetried       = new AtomicLong();
    public final AtomicLong statTimedOut      = new AtomicLong();

    /** 重派时给正常路径留的宽限期（秒），避开「建实例 → 派发」之间的空窗 */
    private static final int RETRY_GRACE_SEC = 5;

    /** 超时判定的宽限（秒）：任务超时后再等这么久才判定，避免网络稍慢就误杀 */
    private static final int TIMEOUT_GRACE_SEC = 10;

    @Scheduled(fixedDelayString = "${scheduler.failover-interval-ms:30000}",
            initialDelayString = "${scheduler.failover-interval-ms:30000}")
    public void run() {
        if (!props.isAdmin()) {
            return;
        }
        try {
            markOffline();
            transferStuck();
            handleTimeout();
            handleMisfire();
        } catch (Exception e) {
            log.error("故障转移执行失败", e);
        }
    }

    /**
     * ★ 执行超时看门狗。
     *
     * <p>回调是网络调用，<b>一定会丢</b> —— 执行器中途被杀、网络抖动、
     * 中心重启，都会让一个实例永远卡在 RUNNING。没有这个看门狗，
     * 任务就"石沉大海"了：既不会成功也不会失败，重试也不会发生。
     *
     * <p>（这个功能我一开始写在设计里但忘了实现，结果压测时真的抓到一个
     * 永远 RUNNING 的实例，才补上。）
     */
    private void handleTimeout() {
        List<JobInstance> timedOut = instanceRepo.findTimeout(TIMEOUT_GRACE_SEC);
        for (JobInstance inst : timedOut) {
            JobInfo job = jobRepo.findById(inst.getJobId());
            int maxRetry = job == null || job.getMaxRetry() == null ? 0 : job.getMaxRetry();
            int used = inst.getRetryCount() == null ? 0 : inst.getRetryCount();
            String msg = "执行超时：超过 " + (job == null ? "?" : job.getTimeoutSec()) + "s 未收到回调";

            if (used < maxRetry) {
                // 还能重试：退回 PENDING，由 retryPending 重新派发
                instanceRepo.markRetry(inst.getId(), msg);
                statTimedOut.incrementAndGet();
                log.warn("实例超时，安排重试 instance={} {}/{}", inst.getId(), used + 1, maxRetry);
            } else {
                instanceRepo.markFinished(inst.getId(), JobInstance.TIMEOUT, msg);
                statTimedOut.incrementAndGet();
                log.error("实例超时且重试用尽 instance={}", inst.getId());
            }
            logRepo.error(inst.getId(), msg);
        }
    }

    /**
     * ④ 重新派发「待执行但没人接」的实例。
     *
     * <p>单独一个调度、跑得比故障转移勤（5 秒），因为它管的是<b>正常路径的补充</b>：
     * 失败重试退回来的实例、以及派发时恰好没有执行器可用的实例。
     *
     * <p>如果没有这一步，「失败重试」就成了空话 —— 实例退回 PENDING 后没有任何人
     * 会去重新派发它。这是我写这个项目时差点漏掉的一环。
     *
     * <p>5 秒的宽限期是为了避开「建实例 → markDispatched」之间的空窗，
     * 不让正常路径被误当成待重派。
     */
    @Scheduled(fixedDelayString = "${scheduler.retry-interval-ms:5000}")
    public void retryPending() {
        if (!props.isAdmin()) {
            return;
        }
        try {
            List<JobInstance> pending = instanceRepo.findPendingUnassigned(RETRY_GRACE_SEC);
            if (pending.isEmpty()) {
                return;
            }
            List<ExecutorNode> healthy = instanceDispatcher.healthyExecutors();
            if (healthy.isEmpty()) {
                return;
            }

            List<JobInfo> jobs = new ArrayList<>();
            List<JobInstance> targets = new ArrayList<>();
            for (JobInstance inst : pending) {
                JobInfo job = jobRepo.findById(inst.getJobId());
                if (job == null || job.getStatus() == null || job.getStatus() != 1) {
                    continue;
                }
                jobs.add(job);
                targets.add(inst);
            }
            if (targets.isEmpty()) {
                return;
            }

            int ok = instanceDispatcher.dispatchBatch(jobs, targets, healthy);
            statRetried.addAndGet(ok);
            log.info("重新派发 {} 个待执行实例（成功 {}）", targets.size(), ok);
        } catch (Exception e) {
            log.error("重新派发失败", e);
        }
    }

    /** ① 心跳超时的执行器标记为离线。 */
    private void markOffline() {
        int n = executorRepo.markOffline(
                props.getExecutor().getAppName(), props.getExecutor().getOfflineThresholdSec());
        if (n > 0) {
            statOfflineMarked.addAndGet(n);
            log.warn("标记 {} 个执行器为离线（心跳超时 {}s）", n, props.getExecutor().getOfflineThresholdSec());
        }
    }

    /**
     * ② 把「卡在离线执行器上」的实例重新路由。
     *
     * <p>这是「执行器宕机后任务不丢」的实现。重新派发时<b>不增加 retry_count</b>，
     * 因为它压根没被执行过，不算一次失败尝试。
     */
    private void transferStuck() {
        List<JobInstance> stuck = instanceRepo.findStuckOnOfflineExecutor(
                props.getExecutor().getOfflineThresholdSec());
        if (stuck.isEmpty()) {
            return;
        }
        List<ExecutorNode> healthy = instanceDispatcher.healthyExecutors();
        if (healthy.isEmpty()) {
            log.warn("有 {} 个实例需要转移，但没有健康执行器", stuck.size());
            return;
        }

        List<JobInfo> jobs = new ArrayList<>();
        List<JobInstance> targets = new ArrayList<>();
        for (JobInstance inst : stuck) {
            JobInfo job = jobRepo.findById(inst.getJobId());
            if (job == null) {
                continue;
            }
            jobs.add(job);
            targets.add(inst);
            logRepo.error(inst.getId(), "原执行器离线，转移中");
        }

        // 并发派发：一次可能有好几十个要转移，串行会被 HTTP 往返卡住
        int ok = instanceDispatcher.dispatchBatch(jobs, targets, healthy);
        statTransferred.addAndGet(ok);
        if (ok > 0) {
            log.warn("故障转移完成：{} 个实例被转移到健康执行器", ok);
        }
    }

    /**
     * ③ misfire 补偿：处理「触发时刻早就过去、但一直没执行」的实例。
     *
     * <p>典型场景：调度中心停了一天，重启后表里躺着一堆 PENDING 的旧实例。
     *
     * <p>两种策略，按任务配置走：
     * <ul>
     *   <li><b>补跑一次</b>（misfire_strategy=1）：适合「必须执行」的任务，
     *       比如每天的对账。补跑一次然后照常。</li>
     *   <li><b>直接跳过</b>（misfire_strategy=2，默认）：适合「过了就过了」的任务，
     *       比如每小时刷一次缓存 —— 补跑十几次毫无意义，还不如等下一次。</li>
     * </ul>
     *
     * <p>这是 Quartz 里的 Misfire 概念，逻辑一样但实现简单得多。
     */
    private void handleMisfire() {
        List<JobInstance> misfired = instanceRepo.findMisfired(props.getMisfireThresholdSec());
        if (misfired.isEmpty()) {
            return;
        }
        List<ExecutorNode> healthy = instanceDispatcher.healthyExecutors();

        // 「只补跑一次」需要按任务去重，所以这里不能简单批量并发派发。
        // misfire 不是热路径（只在中心停机重启后才有），串行足够。
        Set<Long> rerunJobs = new HashSet<>();
        int rerun = 0;
        for (JobInstance inst : misfired) {
            JobInfo job = jobRepo.findById(inst.getJobId());
            if (job == null) {
                continue;
            }
            boolean skip = job.getMisfireStrategy() == null || job.getMisfireStrategy() == 2;
            if (skip) {
                instanceRepo.markFinished(inst.getId(), JobInstance.SKIPPED, "misfire：按策略跳过");
                statMisfireSkipped.incrementAndGet();
                continue;
            }
            // 补跑策略：同一个任务只补跑一次，多出来的错过时刻直接跳过
            if (!rerunJobs.add(job.getId())) {
                instanceRepo.markFinished(inst.getId(), JobInstance.SKIPPED,
                        "misfire：本次仅补跑一次，多余跳过");
                statMisfireSkipped.incrementAndGet();
                continue;
            }
            if (healthy.isEmpty() || !instanceDispatcher.dispatchOne(job, inst, healthy)) {
                // 派发不出去就跳过，不能让它一直挂着
                instanceRepo.markFinished(inst.getId(), JobInstance.SKIPPED,
                        "misfire：没有可用执行器，跳过");
                statMisfireSkipped.incrementAndGet();
                continue;
            }
            rerun++;
            statMisfireFixed.incrementAndGet();
            log.warn("misfire 补跑 instance={} job={} triggerTime={}",
                    inst.getId(), job.getJobName(), inst.getTriggerTime());
            logRepo.error(inst.getId(), "misfire 补跑，计划触发时间 " + inst.getTriggerTime());
        }
        if (rerun > 0) {
            log.warn("misfire 处理完成：补跑 {} 个，跳过 {} 个",
                    rerun, statMisfireSkipped.get());
        }
    }
}
