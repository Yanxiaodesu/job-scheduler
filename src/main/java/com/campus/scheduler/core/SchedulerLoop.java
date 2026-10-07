package com.campus.scheduler.core;

import com.campus.scheduler.config.SchedulerProperties;
import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.repo.JobInfoRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 轮询式调度循环。
 *
 * <h2>它的定位</h2>
 *
 * <p>每秒（可配）扫一次库，把「已经到点」的任务捞出来触发。
 * 逻辑简单、不依赖内存状态、重启即恢复 —— 但精度受扫描间隔限制。
 *
 * <p>现在它有两种角色，取决于 {@code scheduler.prefetch-enabled}：
 *
 * <table border="1">
 *   <tr><th>配置</th><th>角色</th><th>精度</th></tr>
 *   <tr>
 *     <td>{@code prefetch-enabled=false}</td>
 *     <td><b>主调度</b>，建议 scan-interval 设 200ms</td>
 *     <td>P50 ≈ 100ms</td>
 *   </tr>
 *   <tr>
 *     <td>{@code prefetch-enabled=true}（默认）</td>
 *     <td><b>兜底</b>，建议 scan-interval 设 5000ms</td>
 *     <td>由 {@link PrefetchScheduler} 负责，P50 毫秒级</td>
 *   </tr>
 * </table>
 *
 * <p>为什么兜底也要留着：内存队列是「易失状态」，进程重启就没了。
 * 而且预取万一漏了某个任务，没有兜底就永远不会被触发。
 * 两条路重叠触发无害 —— 抢占走的是同一套唯一键 + 乐观锁。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SchedulerLoop {

    private final SchedulerProperties props;
    private final JobInfoRepo jobRepo;
    private final JobTrigger trigger;

    /** 一次扫多少条。太多会拖慢循环，剩下的下一秒再处理 */
    private static final int SCAN_BATCH = 200;

    /** 扫描轮次。用它除以运行时长就是 DB 查询频率 —— 精度优化的代价指标 */
    public final java.util.concurrent.atomic.AtomicLong statScanRounds =
            new java.util.concurrent.atomic.AtomicLong();

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
                trigger.fire(job, "polling");
            } catch (Exception e) {
                // 单个任务出问题不能影响后面的任务
                log.error("触发任务失败 job={} id={}", job.getJobName(), job.getId(), e);
            }
        }
    }
}
