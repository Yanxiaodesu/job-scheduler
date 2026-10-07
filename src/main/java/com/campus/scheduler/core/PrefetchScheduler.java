package com.campus.scheduler.core;

import com.campus.scheduler.config.SchedulerProperties;
import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.repo.JobInfoRepo;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ★ 内存延迟队列调度 —— 突破「轮询扫描」的精度上限。
 *
 * <h2>为什么要做这个</h2>
 *
 * <p>轮询式扫描（{@link SchedulerLoop}）有个绕不过去的数学规律：
 * <b>平均延迟 ≈ 扫描间隔的一半</b>。实测：
 *
 * <table border="1">
 *   <tr><th>扫描间隔</th><th>P50 延迟</th><th>DB 查询</th></tr>
 *   <tr><td>1000 ms</td><td>561 ms</td><td>~1 次/秒</td></tr>
 *   <tr><td>200 ms</td><td>108 ms</td><td>~6.4 次/秒</td></tr>
 * </table>
 *
 * <p>想把精度再往上提，就只能继续缩短间隔 —— 但那会让 DB 查询频率线性上涨，
 * 任务一多就先把数据库压垮了。<b>这条路走不通。</b>
 *
 * <h2>换一条路</h2>
 *
 * <p>不在「到点」的时候才发现任务，而是<b>提前把即将到点的任务装进内存</b>：
 *
 * <pre>
 *   预取线程   每 1 秒查一次「未来 3 秒内到点的任务」→ 放进 DelayQueue
 *   工作线程   queue.take() 阻塞等待 → 到点自动醒来 → 触发
 * </pre>
 *
 * <p>收益：
 * <ul>
 *   <li><b>精度</b>：不再受扫描间隔限制，取决于 {@code DelayQueue} 的唤醒精度（毫秒级）</li>
 *   <li><b>DB 压力</b>：查询频率从「每秒几次」降到「每秒 1 次」，而且扫的是索引区间</li>
 *   <li><b>空闲零开销</b>：没有任务到点时，工作线程阻塞在 {@code take()} 上，不消耗 CPU</li>
 * </ul>
 *
 * <h2>怎么保证不和轮询路径打架</h2>
 *
 * <p>两条路可以<b>同时开着</b>。因为「抢占一次触发」这件事统一走
 * {@link JobTrigger#fire} —— 唯一键 + 乐观锁保证谁先抢到算谁的，
 * 另一个自然落空。不需要任何额外的协调机制。
 *
 * <p>所以推荐配置是 <b>prefetch 主跑 + polling 兜底</b>：
 * 内存队列负责精度，轮询用一个较长的间隔（如 5 秒）兜住
 * 「预取漏了 / 队列异常」的情况。两条路重叠触发也无害。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PrefetchScheduler {

    private final SchedulerProperties props;
    private final JobInfoRepo jobRepo;
    private final JobTrigger trigger;

    /** 到点队列。{@code DelayQueue} 会自动把最近到点的排在队首 */
    private final DelayQueue<DueJob> queue = new DelayQueue<>();

    /**
     * 已经装进队列的 {@code jobId:触发时刻}。
     *
     * <p>为什么需要：预取是每 1 秒查一次「未来 3 秒内到点的任务」，
     * 同一个任务会被连续查出好几轮。没有这个集合就会重复入队。
     * 触发后把它移除，下次排期到了自然会被重新装进来。
     */
    private final Set<String> enqueued = ConcurrentHashMap.newKeySet();

    private volatile boolean running = false;
    private Thread worker;

    public final AtomicLong statPrefetched = new AtomicLong();
    public final AtomicLong statFired = new AtomicLong();

    /** 队列里的元素：一个「什么时候该触发哪个任务」 */
    private record DueJob(JobInfo job, long dueAtMillis, String key) implements Delayed {
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(dueAtMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(dueAtMillis, ((DueJob) other).dueAtMillis);
        }
    }

    @PostConstruct
    public void start() {
        if (!props.isAdmin() || !props.isPrefetchEnabled()) {
            return;
        }
        running = true;
        worker = new Thread(this::loop, "prefetch-scheduler");
        worker.setDaemon(true);
        worker.start();
        log.info("内存延迟队列调度已启动：预取间隔 {}ms，预取窗口 {}s",
                props.getPrefetchIntervalMs(), props.getPrefetchWindowSec());
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (worker != null) {
            worker.interrupt();
        }
    }

    /**
     * 预取：把「未来一小段时间内将要到点」的任务装进内存队列。
     *
     * <p>窗口取 3 秒、间隔 1 秒，也就是说每个任务会被反复查到 2~3 次 ——
     * 靠 {@code enqueued} 去重。窗口留够冗余是故意的：
     * 万一某次预取查询慢了几百毫秒，也不会漏掉任务。
     */
    @Scheduled(fixedDelayString = "${scheduler.prefetch-interval-ms:1000}")
    public void prefetch() {
        if (!props.isAdmin() || !props.isPrefetchEnabled()) {
            return;
        }
        try {
            List<JobInfo> upcoming = jobRepo.findUpcoming(props.getPrefetchWindowSec(), 1000);
            int added = 0;
            for (JobInfo job : upcoming) {
                if (job.getNextTriggerTime() == null) {
                    continue;
                }
                String key = job.getId() + ":" + job.getNextTriggerTime();
                if (!enqueued.add(key)) {
                    continue;   // 已经在队列里了
                }
                long dueAt = job.getNextTriggerTime()
                        .atZone(java.time.ZoneId.systemDefault())
                        .toInstant().toEpochMilli();
                queue.offer(new DueJob(job, dueAt, key));
                added++;
            }
            if (added > 0) {
                statPrefetched.addAndGet(added);
                log.debug("预取 {} 个任务进内存队列，队列长度 {}", added, queue.size());
            }
        } catch (Exception e) {
            // 预取失败不能让线程死掉：轮询路径还在兜底
            log.error("预取失败（轮询路径仍在兜底）", e);
        }
    }

    /**
     * 工作线程：阻塞等待下一个到点的任务。
     *
     * <p>用 {@code take()} 而不是「每 10 毫秒看一眼队首」——
     * 后者本质上还是轮询，虽然不查库，但会白白占着 CPU。
     * {@code take()} 在没有任务时是真的挂起，空闲时 CPU 占用为 0。
     */
    private void loop() {
        while (running) {
            try {
                DueJob due = queue.take();          // 阻塞到下一个到点
                enqueued.remove(due.key());
                try {
                    if (trigger.fire(due.job(), "prefetch")) {
                        statFired.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("触发失败 job={}", due.job().getJobName(), e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // 单个任务出问题不能让整个工作线程退出
                log.error("内存队列调度循环异常", e);
            }
        }
    }

    /** 队列当前长度，给指标用 */
    public int queueSize() {
        return queue.size();
    }

    /** 已入队去重集合大小，给指标和测试用 */
    public int pendingKeys() {
        return enqueued.size();
    }
}
