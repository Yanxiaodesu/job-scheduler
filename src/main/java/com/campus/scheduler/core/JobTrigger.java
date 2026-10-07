package com.campus.scheduler.core;

import com.campus.scheduler.model.ExecutorNode;
import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.model.JobInstance;
import com.campus.scheduler.repo.JobInfoRepo;
import com.campus.scheduler.repo.JobInstanceRepo;
import com.campus.scheduler.repo.JobLogRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 「触发一次任务」这件事的完整实现。
 *
 * <h2>为什么单独抽出来</h2>
 *
 * <p>现在有<b>两种调度策略</b>，但「怎么抢占一次触发」必须完全一致：
 *
 * <ul>
 *   <li>{@link SchedulerLoop} —— 轮询扫表（简单、精度受扫描间隔限制）</li>
 *   <li>{@link PrefetchScheduler} —— 内存延迟队列（精度高、DB 压力小）</li>
 * </ul>
 *
 * <p>两条路共用这里的抢占逻辑，所以它们可以<b>同时开着</b>而不会重复派发 ——
 * 谁先抢到算谁的，另一个自然落空。这一点是靠唯一键 + 乐观锁天然获得的，
 * 不需要额外的协调。
 *
 * <h2>抢占顺序（重要）</h2>
 *
 * <pre>
 *   ① 先落实例（唯一键即锁）
 *   ② 再推进 next_trigger_time（乐观锁）
 *   ③ 最后派发
 * </pre>
 *
 * <p>反过来写的话，推进成功后进程崩溃，这次触发就永远丢了。
 * 另外②这一步<b>抢输的一方也要执行</b>，否则赢家在推进前崩溃会让任务
 * 卡在同一个时刻反复空转（详见 README「修过的坑」）。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JobTrigger {

    private final JobInfoRepo jobRepo;
    private final JobInstanceRepo instanceRepo;
    private final JobLogRepo logRepo;
    private final InstanceDispatcher instanceDispatcher;

    // ---- 计数器：既是运行时指标，也是压测的证据 ----
    public final AtomicLong statSeenDue      = new AtomicLong();
    public final AtomicLong statTriggered    = new AtomicLong();
    public final AtomicLong statLostTheRace  = new AtomicLong();
    public final AtomicLong statNoExecutor   = new AtomicLong();
    public final AtomicLong statDispatchFail = new AtomicLong();

    /**
     * 尝试触发一次。
     *
     * @param source 谁触发的（polling / prefetch），只用于日志排查
     * @return true 表示这次触发被本实例抢到并已派发
     */
    public boolean fire(JobInfo job, String source) {
        statSeenDue.incrementAndGet();

        LocalDateTime triggerTime = job.getNextTriggerTime();
        if (triggerTime == null) {
            return false;
        }
        String instanceKey = job.getId() + ":" + triggerTime;

        // ---------- ① 抢这次触发：唯一键就是锁 ----------
        Long instanceId = instanceRepo.tryCreate(job.getId(), triggerTime, instanceKey);

        // ---------- ② 推进 next_trigger_time（带旧值条件 = 乐观锁）----------
        // 无论上面抢没抢到都要尝试推进：抢输的人也推一次，
        // 是为了兜住「抢到的人推进前崩了」的情况
        LocalDateTime next = CronSupport.next(job.getCronExpr(), LocalDateTime.now());
        jobRepo.advanceTriggerTime(job.getId(), triggerTime, next);

        if (instanceId == null) {
            statLostTheRace.incrementAndGet();
            log.debug("[{}] 触发被他人抢占，跳过 job={} triggerTime={}",
                    source, job.getJobName(), triggerTime);
            return false;
        }

        // ---------- ③ 路由 + 派发 ----------
        List<ExecutorNode> healthy = instanceDispatcher.healthyExecutors();
        if (healthy.isEmpty()) {
            statNoExecutor.incrementAndGet();
            log.warn("[{}] 没有可用执行器，实例留在待执行 job={} instance={}",
                    source, job.getJobName(), instanceId);
            logRepo.error(instanceId, "没有可用执行器，等待执行器上线后重试");
            return false;
        }

        JobInstance instance = instanceRepo.findById(instanceId);
        boolean ok = instanceDispatcher.dispatchOne(job, instance, healthy);
        if (ok) {
            statTriggered.incrementAndGet();
            log.info("[{}] 已派发 job={} instance={} triggerTime={}",
                    source, job.getJobName(), instanceId, triggerTime);
            logRepo.info(instanceId, "已派发（来源 " + source + "）");
        } else {
            statDispatchFail.incrementAndGet();
            log.warn("[{}] 派发失败，已退回待执行 job={} instance={}",
                    source, job.getJobName(), instanceId);
        }
        return ok;
    }
}
