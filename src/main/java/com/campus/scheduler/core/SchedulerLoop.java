package com.campus.scheduler.core;

import com.campus.scheduler.config.SchedulerProperties;
import com.campus.scheduler.model.ExecutorNode;
import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.model.JobInstance;
import com.campus.scheduler.repo.JobInfoRepo;
import com.campus.scheduler.repo.JobInstanceRepo;
import com.campus.scheduler.repo.JobLogRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ★★★ 调度循环 —— 整个项目的核心。
 *
 * <h2>它每一秒做四件事</h2>
 * <ol>
 *   <li>捞出「到点」的任务（{@code next_trigger_time <= NOW()}）</li>
 *   <li><b>抢</b>这次触发：先落实例（唯一键），再推进下次触发时间（乐观锁）</li>
 *   <li>路由到一个健康的执行器</li>
 *   <li>派发（HTTP）</li>
 * </ol>
 *
 * <h2>「恰好一次」是怎么保证的 —— 面试必问</h2>
 *
 * <p>假设起了 3 个 Admin 实例，它们同时扫到了同一个任务。三层防护：
 *
 * <table border="1">
 *   <tr><th>层</th><th>手段</th><th>拦住什么</th></tr>
 *   <tr>
 *     <td>1</td>
 *     <td>{@code job_instance} 的 UNIQUE KEY (instance_key)</td>
 *     <td>同一任务 + 同一触发时刻，数据库只允许落一条实例</td>
 *   </tr>
 *   <tr>
 *     <td>2</td>
 *     <td>{@code UPDATE job_info SET next_trigger_time=? WHERE id=? AND next_trigger_time=?}</td>
 *     <td>乐观锁（CAS）。旧值对不上就影响 0 行，防止把同一个触发时刻推进两次</td>
 *   </tr>
 *   <tr>
 *     <td>3</td>
 *     <td>执行器侧用 instanceId 幂等</td>
 *     <td>网络重试导致的重复派发</td>
 *   </tr>
 * </table>
 *
 * <p><b>注意这里没有引入 Redis 分布式锁</b>，这是刻意的取舍：
 * 少一个组件就少一个故障点，而调度频率只有秒级，数据库完全扛得住。
 * 代价是高并发下失败的一方要重试、且强依赖数据库可用性 —— 面试时要把这个代价说出来。
 *
 * <h2>一个容易写错的地方</h2>
 *
 * <p>顺序必须是「<b>先落实例，再推进时间</b>」，而且<b>抢输的一方也要试着推进时间</b>。
 * 原因是：如果抢到的人在「落实例」和「推进时间」之间崩了，
 * next_trigger_time 还停在旧值 → 下一轮又会被扫到 → 又去抢实例（重复键，失败）……
 * 如果失败的一方不推进时间，这个任务就会<b>永远卡在这一刻反复空转</b>。
 * 让失败方也执行一次带条件的推进，就自然收敛了。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SchedulerLoop {

    private final SchedulerProperties props;
    private final JobInfoRepo jobRepo;
    private final JobInstanceRepo instanceRepo;
    private final JobLogRepo logRepo;
    private final InstanceDispatcher instanceDispatcher;

    /** 一次扫多少条。太多会拖慢循环，剩下的下一秒再处理 */
    private static final int SCAN_BATCH = 200;

    // ---- 计数器：既是运行时的可观测指标，也是压测「零重复」的证据 ----
    public final AtomicLong statScanRounds   = new AtomicLong();
    public final AtomicLong statSeenDue      = new AtomicLong();
    public final AtomicLong statTriggered    = new AtomicLong();  // 真正抢到并派发的
    public final AtomicLong statLostTheRace  = new AtomicLong();  // 抢实例失败（说明并发防护生效）
    public final AtomicLong statNoExecutor   = new AtomicLong();
    public final AtomicLong statDispatchFail = new AtomicLong();

    @Scheduled(fixedDelayString = "${scheduler.scan-interval-ms:1000}")
    public void scan() {
        if (!props.isAdmin()) {
            return;
        }
        statScanRounds.incrementAndGet();

        List<JobInfo> due;
        try {
            due = jobRepo.findDue(SCAN_BATCH);
        } catch (Exception e) {
            // 数据库抖一下不该让整个调度停摆，记录下来等下一轮
            log.error("扫描到点任务失败", e);
            return;
        }

        for (JobInfo job : due) {
            try {
                trigger(job);
            } catch (Exception e) {
                // 单个任务出问题不能影响后面的任务
                log.error("触发任务失败 job={} id={}", job.getJobName(), job.getId(), e);
            }
        }
    }

    /**
     * 触发一个任务。核心流程见类注释。
     */
    private void trigger(JobInfo job) {
        statSeenDue.incrementAndGet();

        LocalDateTime triggerTime = job.getNextTriggerTime();
        if (triggerTime == null) {
            return;
        }
        String instanceKey = job.getId() + ":" + triggerTime;

        // ---------- ① 抢这次触发：唯一键就是锁 ----------
        // 落得进去说明这个触发时刻归我；落不进去说明别的 Admin 已经抢了
        Long instanceId = instanceRepo.tryCreate(job.getId(), triggerTime, instanceKey);

        // ---------- ② 推进 next_trigger_time（带旧值条件 = 乐观锁）----------
        // 注意：无论上面抢没抢到，都要尝试推进。
        // 抢到的人负责推进是正常的；抢输的人也要推 —— 那是为了兜住
        // 「抢到的人推进前崩了」的情况，否则任务会卡在这一刻反复空转（见类注释）。
        LocalDateTime next = CronSupport.next(job.getCronExpr(), LocalDateTime.now());
        jobRepo.advanceTriggerTime(job.getId(), triggerTime, next);

        if (instanceId == null) {
            // 这次触发不归我 —— 这是正常的并发结果，不是错误
            statLostTheRace.incrementAndGet();
            log.debug("触发被他人抢占，跳过 job={} triggerTime={}", job.getJobName(), triggerTime);
            return;
        }

        // ---------- ③④ 路由 + 派发 ----------
        // 逻辑抽在 InstanceDispatcher 里，和故障转移、手动触发共用一份
        List<ExecutorNode> healthy = instanceDispatcher.healthyExecutors();
        if (healthy.isEmpty()) {
            // 没有可用执行器。**不标记失败**，留在 PENDING 等执行器上线
            statNoExecutor.incrementAndGet();
            log.warn("没有可用执行器，实例留在待执行 job={} instance={}", job.getJobName(), instanceId);
            logRepo.error(instanceId, "没有可用执行器，等待执行器上线后重试");
            return;
        }

        JobInstance instance = instanceRepo.findById(instanceId);
        boolean ok = instanceDispatcher.dispatchOne(job, instance, healthy);
        if (ok) {
            statTriggered.incrementAndGet();
            log.info("已派发 job={} instance={}", job.getJobName(), instanceId);
            logRepo.info(instanceId, "已派发");
        } else {
            statDispatchFail.incrementAndGet();
            log.warn("派发失败，留给故障转移重试 job={} instance={}", job.getJobName(), instanceId);
            logRepo.error(instanceId, "派发失败，等待重试");
        }
    }
}
