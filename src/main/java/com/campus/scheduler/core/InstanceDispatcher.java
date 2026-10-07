package com.campus.scheduler.core;

import com.campus.scheduler.config.SchedulerProperties;
import com.campus.scheduler.model.ExecutorNode;
import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.model.JobInstance;
import com.campus.scheduler.repo.ExecutorRepo;
import com.campus.scheduler.repo.JobInstanceRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 把实例派发给执行器。
 *
 * <h2>为什么单独抽一个类</h2>
 *
 * <p>「查健康执行器 → 路由选一个 → 标记已派发 → HTTP 派发」这四步，
 * 原本在调度循环、故障转移、重新派发三处各写了一遍。
 * 抽出来之后逻辑只有一份，改的时候不会漏。
 *
 * <h2>为什么要有批量派发（并发）</h2>
 *
 * <p>派发是<b>I/O 密集型</b>操作 —— 每一步都在等 HTTP 往返。串行做的话，
 * 吞吐上限就是「1 / 单次往返时间」。
 *
 * <p>实测：300 个任务串行派发要 66 秒（约 4.5 个/秒），
 * 并发派发后降到个位数秒。这是这个项目里最明显的一处性能问题。
 *
 * <p>并发度用固定线程池而不是 {@code parallelStream()}：后者会用公共的
 * ForkJoinPool，把一个后台任务的负载扩散到整个 JVM，不合适。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InstanceDispatcher {

    private final SchedulerProperties props;
    private final ExecutorRepo executorRepo;
    private final JobInstanceRepo instanceRepo;
    private final Router router;
    private final Dispatcher dispatcher;

    /** 派发线程池。派发是 I/O 等待，线程数可以远大于 CPU 核数 */
    private final ExecutorService pool = Executors.newFixedThreadPool(16);

    public final AtomicLong statDispatched = new AtomicLong();
    public final AtomicLong statFailed = new AtomicLong();

    /** 取当前健康的执行器 */
    public List<ExecutorNode> healthyExecutors() {
        return executorRepo.findHealthy(
                props.getExecutor().getAppName(), props.getExecutor().getOfflineThresholdSec());
    }

    /**
     * 派发单个实例（同步）。
     *
     * @return true 表示执行器已接收。false 表示没有可用执行器、或 HTTP 失败，
     *         调用方应当把这个实例留在 PENDING 等下次重派
     */
    public boolean dispatchOne(JobInfo job, JobInstance inst, List<ExecutorNode> healthy) {
        ExecutorNode target = router.pick(healthy);
        if (target == null) {
            return false;
        }
        // 先标记再发：标记为 RUNNING 之后，重派扫描就不会再捡到它，
        // 避免和正常路径重复派发
        instanceRepo.markDispatched(inst.getId(), target.getId());
        boolean ok = dispatcher.dispatch(target, job, inst);
        if (ok) {
            statDispatched.incrementAndGet();
            return true;
        }
        // ★ HTTP 没送达：必须把它退回 PENDING 并清空 executor_id，
        //   否则它会卡在「执行中 + 已分配执行器」这个两边都捞不到的状态，
        //   只能等超时看门狗兜底（见 JobInstanceRepo#revertToPending 的说明）
        instanceRepo.revertToPending(inst.getId(),
                "派发到 " + target.getAddress() + " 失败，等待重新派发");
        statFailed.incrementAndGet();
        return false;
    }

    public boolean dispatchOne(JobInfo job, JobInstance inst) {
        return dispatchOne(job, inst, healthyExecutors());
    }

    /**
     * ★ 批量派发（并发）。
     *
     * <p>把一批实例丢进线程池同时派发，而不是一个一个等。
     * 故障转移、手动触发、重派这些「一次可能有好几十上百个」的场景都走这里。
     *
     * @return 成功派发的数量
     */
    public int dispatchBatch(List<JobInfo> jobs, List<JobInstance> instances,
                             List<ExecutorNode> healthy) {
        if (instances.isEmpty() || healthy.isEmpty()) {
            return 0;
        }
        List<Future<Boolean>> futures = new ArrayList<>(instances.size());
        for (int i = 0; i < instances.size(); i++) {
            JobInfo job = jobs.get(i);
            JobInstance inst = instances.get(i);
            futures.add(pool.submit(() -> dispatchOne(job, inst, healthy)));
        }
        int ok = 0;
        for (Future<Boolean> f : futures) {
            try {
                if (Boolean.TRUE.equals(f.get())) {
                    ok++;
                }
            } catch (Exception e) {
                // 单个失败不影响其他：它留在 PENDING 会被下次重派捡走
                log.warn("批量派发中有实例失败: {}", e.getMessage());
            }
        }
        return ok;
    }

    public void shutdown() {
        pool.shutdown();
    }
}
