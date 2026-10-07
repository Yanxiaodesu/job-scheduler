package com.campus.scheduler.api;

import com.campus.scheduler.config.SchedulerProperties;
import com.campus.scheduler.core.FailoverTask;
import com.campus.scheduler.core.SchedulerLoop;
import com.campus.scheduler.model.ExecutorNode;
import com.campus.scheduler.model.JobInstance;
import com.campus.scheduler.repo.ExecutorRepo;
import com.campus.scheduler.repo.JobInstanceRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 运行指标。
 *
 * <p>压测「多 Admin 并发下不重复派发」时，就是靠这里看结果的：
 * {@code lostTheRace} 应该等于「其他 Admin 实例成功派发的次数」，
 * 而每个触发时刻只应该产生一条实例。
 */
@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final SchedulerProperties props;
    private final SchedulerLoop scheduler;
    private final FailoverTask failover;
    private final ExecutorRepo executorRepo;
    private final JobInstanceRepo instanceRepo;
    private final com.campus.scheduler.api.CallbackController callback;
    private final com.campus.scheduler.executor.ExecutorEndpoint executorEndpoint;
    private final com.campus.scheduler.core.InstanceDispatcher instanceDispatcher;
    private final com.campus.scheduler.core.JobTrigger trigger;
    private final com.campus.scheduler.core.PrefetchScheduler prefetch;

    @GetMapping
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("role", props.getRole());
        m.put("instanceId", props.getInstanceId());
        m.put("appName", props.getExecutor().getAppName());
        // 扫描间隔直接决定调度精度，压测脚本要读它来解释测出来的延迟
        m.put("scanIntervalMs", props.getScanIntervalMs());

        Map<String, Object> sched = new LinkedHashMap<>();
        sched.put("scanRounds", scheduler.statScanRounds.get());
        sched.put("seenDue", trigger.statSeenDue.get());
        sched.put("triggered", trigger.statTriggered.get());
        // ★ 这个数字是关键证据：>0 说明并发防护真的拦住了重复派发
        sched.put("lostTheRace", trigger.statLostTheRace.get());
        sched.put("noExecutor", trigger.statNoExecutor.get());
        sched.put("dispatchFailed", trigger.statDispatchFail.get());
        m.put("scheduler", sched);

        // 内存延迟队列（预取）的状态
        Map<String, Object> pf = new LinkedHashMap<>();
        pf.put("enabled", props.isPrefetchEnabled());
        pf.put("queueSize", prefetch.queueSize());
        pf.put("prefetched", prefetch.statPrefetched.get());
        pf.put("firedFromQueue", prefetch.statFired.get());
        pf.put("prefetchWindowSec", props.getPrefetchWindowSec());
        m.put("prefetch", pf);

        Map<String, Object> fo = new LinkedHashMap<>();
        fo.put("offlineMarked", failover.statOfflineMarked.get());
        fo.put("transferred", failover.statTransferred.get());
        fo.put("retried", failover.statRetried.get());
        fo.put("timedOut", failover.statTimedOut.get());
        fo.put("misfireRerun", failover.statMisfireFixed.get());
        fo.put("misfireSkipped", failover.statMisfireSkipped.get());
        m.put("failover", fo);

        Map<String, Object> cb = new LinkedHashMap<>();
        cb.put("callback", callback.statCallback.get());
        cb.put("retryScheduled", callback.statRetry.get());
        cb.put("staleIgnored", callback.statStaleCallback.get());
        m.put("callback", cb);

        Map<String, Object> dp = new LinkedHashMap<>();
        dp.put("dispatched", instanceDispatcher.statDispatched.get());
        dp.put("failed", instanceDispatcher.statFailed.get());
        m.put("dispatcher", dp);

        Map<String, Object> ex = new LinkedHashMap<>();
        ex.put("received", executorEndpoint.statReceived.get());
        ex.put("success", executorEndpoint.statSuccess.get());
        ex.put("failed", executorEndpoint.statFailed.get());
        ex.put("duplicateIgnored", executorEndpoint.statDuplicate.get());
        m.put("executor", ex);

        m.put("instances", Map.of(
                "pending", instanceRepo.countByStatus(JobInstance.PENDING),
                "running", instanceRepo.countByStatus(JobInstance.RUNNING),
                "success", instanceRepo.countByStatus(JobInstance.SUCCESS),
                "failed", instanceRepo.countByStatus(JobInstance.FAILED),
                "timeout", instanceRepo.countByStatus(JobInstance.TIMEOUT),
                "skipped", instanceRepo.countByStatus(JobInstance.SKIPPED)));

        List<ExecutorNode> nodes = executorRepo.findAll();
        m.put("executors", nodes);
        return m;
    }
}
