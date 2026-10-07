package com.campus.scheduler.api;

import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.model.JobInstance;
import com.campus.scheduler.repo.JobInfoRepo;
import com.campus.scheduler.repo.JobInstanceRepo;
import com.campus.scheduler.repo.JobLogRepo;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 接收执行器回传的执行结果。
 *
 * <p>这是「失败重试」真正发生的地方：执行器报告失败后，
 * 由这里判断「还能不能重试」，而不是让执行器自己重试 ——
 * <b>重试决策必须由中心统一做</b>，否则多个执行器各自重试就会失控。
 */
@Slf4j
@RestController
@RequestMapping("/api/internal")
@RequiredArgsConstructor
public class CallbackController {

    private final JobInfoRepo jobRepo;
    private final JobInstanceRepo instanceRepo;
    private final JobLogRepo logRepo;

    public final AtomicLong statCallback = new AtomicLong();
    public final AtomicLong statRetry = new AtomicLong();
    /** 被判定为迟到/重复而丢弃的回调数。这个数应该是小的，且不导致业务错乱 */
    public final AtomicLong statStaleCallback = new AtomicLong();

    @Data
    public static class CallbackRequest {
        private Long instanceId;
        private Integer status;
        private String message;
        private Long elapsedMs;
        /** 这是第几次尝试的回调。用来丢弃迟到/重复的回调 */
        private Integer retryCount;
    }

    @PostMapping("/callback")
    public ResponseEntity<Map<String, Object>> callback(@RequestBody CallbackRequest req) {
        statCallback.incrementAndGet();

        JobInstance inst = instanceRepo.findById(req.getInstanceId());
        if (inst == null) {
            log.warn("回调的实例不存在 instance={}", req.getInstanceId());
            return ResponseEntity.ok(Map.of("ok", false, "msg", "unknown instance"));
        }

        // ---------- 幂等守卫，两道 ----------
        // ① 只接受「执行中」的实例。
        //    这里**不能**放 PENDING 通过 —— 曾经放过，结果一条迟到的回调
        //    会对着已经是 PENDING（等重试）的实例再跑一次 markRetry，
        //    白白把 retry_count 加了一次，导致重试次数和实际尝试次数对不上。
        if (inst.getStatus() != JobInstance.RUNNING) {
            statStaleCallback.incrementAndGet();
            log.warn("实例状态={} 非执行中，忽略回调 instance={}", inst.getStatus(), req.getInstanceId());
            return ResponseEntity.ok(Map.of("ok", true, "msg", "not running, ignored"));
        }

        // ② 回调里的 retryCount 必须和库里的一致。
        //    否则说明这是「上一次尝试」的迟到回调 —— 上一次的实例早就重试过、
        //    retry_count 已经加过了，再处理一次就会重复计数。
        if (req.getRetryCount() != null
                && !req.getRetryCount().equals(inst.getRetryCount() == null ? 0 : inst.getRetryCount())) {
            statStaleCallback.incrementAndGet();
            log.warn("回调的 retryCount={} 与库中 {} 不一致，判定为迟到回调 instance={}",
                    req.getRetryCount(), inst.getRetryCount(), req.getInstanceId());
            return ResponseEntity.ok(Map.of("ok", true, "msg", "stale callback, ignored"));
        }

        JobInfo job = jobRepo.findById(inst.getJobId());
        String msg = req.getMessage() == null ? "" : req.getMessage();
        int status = req.getStatus() == null ? JobInstance.FAILED : req.getStatus();

        if (status == JobInstance.SUCCESS) {
            instanceRepo.markFinished(inst.getId(), JobInstance.SUCCESS, msg);
            log.info("任务成功 instance={} 耗时={}ms", inst.getId(), req.getElapsedMs());
            return ResponseEntity.ok(Map.of("ok", true));
        }

        // ---------- 失败：判断还能不能重试 ----------
        int maxRetry = job == null || job.getMaxRetry() == null ? 0 : job.getMaxRetry();
        int used = inst.getRetryCount() == null ? 0 : inst.getRetryCount();

        if (used < maxRetry) {
            // 退回 PENDING，由 RetryDispatcher 重新派发。
            // 注意**不是**在这里立刻重试 —— 中心只做决策，派发交给统一入口，
            // 否则「谁负责重派」会散落在多个地方。
            instanceRepo.markRetry(inst.getId(), msg);
            statRetry.incrementAndGet();
            log.warn("任务失败，安排重试 instance={} 已重试 {}/{} 次",
                    inst.getId(), used + 1, maxRetry);
            logRepo.error(inst.getId(), "失败：" + msg + "，将重试 " + (used + 1) + "/" + maxRetry);
        } else {
            instanceRepo.markFinished(inst.getId(), JobInstance.FAILED,
                    msg + "（重试已用尽 " + used + "/" + maxRetry + "）");
            log.error("任务最终失败 instance={} 重试次数用尽 {}/{}", inst.getId(), used, maxRetry);
        }
        return ResponseEntity.ok(Map.of("ok", true));
    }
}
