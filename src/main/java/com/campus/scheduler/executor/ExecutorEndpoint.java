package com.campus.scheduler.executor;

import com.campus.scheduler.config.SchedulerProperties;
import com.campus.scheduler.model.JobInstance;
import jakarta.annotation.PreDestroy;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 执行器入口：接收派发 → 异步执行 → 回调结果。
 *
 * <h2>为什么是「异步执行 + 回调」，而不是直接 HTTP 返回结果</h2>
 *
 * <p>如果同步返回，那调度中心和执行器之间就变成了一次长请求 ——
 * 任务跑 5 分钟，这个 HTTP 连接就得挂 5 分钟，中间任何网络抖动都会
 * 让调度中心误以为失败。异步 + 回调把「派发」和「执行结果」解耦：
 * 派发是一次短请求（几毫秒），结果通过回调单独上报。
 *
 * <p>这也是 xxl-job / PowerJob 的做法。
 *
 * <h2>幂等</h2>
 *
 * <p>网络重试可能导致同一个 instanceId 被派发两次。这里用一个
 * 已执行集合挡住（真实生产应该落库或用 Redis，用内存集合重启就失效了 ——
 * 这是个<b>已知的简化</b>，面试被问到要如实说）。
 */
@Slf4j
@RestController
@RequestMapping("/executor")
@RequiredArgsConstructor
public class ExecutorEndpoint {

    private final SchedulerProperties props;
    private final DemoHandlers handlers;

    private final RestClient client = RestClient.builder().build();

    /** 执行线程池。8 个并发执行位 */
    private final ExecutorService pool = Executors.newFixedThreadPool(8);

    /**
     * 幂等：已经执行过的「实例 + 重试次数」。
     *
     * <p><b>为什么键里必须带 retryCount</b>：失败重试派发的是<b>同一个 instanceId</b>，
     * 如果只用 instanceId 去重，第一次失败后的重试会被这里当成「重复派发」直接丢掉，
     * 重试就静默失效了（写这个项目时真的踩了这个坑，见 README「修过的坑」）。
     * 带上重试次数，每次重试就是一个新键，去重只挡住真正的网络重发。
     *
     * <p>生产环境这个集合应该换成 DB 唯一键或 Redis（带 TTL），
     * 因为内存集合一重启就失效 —— 这是个已知的简化。
     */
    private final java.util.Set<String> executed = java.util.concurrent.ConcurrentHashMap.newKeySet();

    public final AtomicLong statReceived = new AtomicLong();
    public final AtomicLong statSuccess = new AtomicLong();
    public final AtomicLong statFailed = new AtomicLong();
    public final AtomicLong statDuplicate = new AtomicLong();

    /** 派发请求体，字段和 admin 侧 Dispatcher 发出来的一致 */
    @Data
    public static class DispatchRequest {
        private Long instanceId;
        private Long jobId;
        private String jobName;
        private Integer handlerType;
        private String handlerValue;
        /** 传给 handler 的参数 */
        private String param;
        private Integer timeoutSec;
        private String callbackUrl;
        /** 第几次尝试（0 表示首次）。用来和 instanceId 一起做去重键 */
        private Integer retryCount;
    }

    @PostMapping("/run")
    public ResponseEntity<Map<String, Object>> run(@RequestBody DispatchRequest req) {
        if (!props.isExecutor()) {
            // 本实例没开执行器角色，直接拒绝，让 Admin 去挑别的节点
            return ResponseEntity.status(503).body(Map.of("ok", false, "msg", "本实例未启用执行器角色"));
        }

        // 幂等：同一个「实例 + 尝试次数」被派发两次，第二次直接忽略。
        // 注意键里带了 retryCount，否则失败重试会被误判成重复派发（见字段注释）
        String dedupKey = req.getInstanceId() + ":" + req.getRetryCount();
        if (!executed.add(dedupKey)) {
            statDuplicate.incrementAndGet();
            log.warn("重复派发，已忽略 instance={} retryCount={}", req.getInstanceId(), req.getRetryCount());
            return ResponseEntity.ok(Map.of("ok", true, "msg", "duplicate, ignored"));
        }

        statReceived.incrementAndGet();
        log.info("收到任务 instance={} job={} handler={}",
                req.getInstanceId(), req.getJobName(), req.getHandlerValue());

        // 立刻返回，实际执行丢到线程池 —— 这就是异步化
        pool.submit(() -> execute(req));
        return ResponseEntity.ok(Map.of("ok", true, "msg", "accepted"));
    }

    private void execute(DispatchRequest req) {
        long start = System.currentTimeMillis();
        int status;
        String msg;
        try {
            if (req.getHandlerType() != null && req.getHandlerType() == 2) {
                // HTTP 回调型：把请求转发给业务方的 URL
                msg = callHttpHandler(req.getHandlerValue(), req);
            } else {
                JobHandler h = handlers.get(req.getHandlerValue());
                if (h == null) {
                    throw new IllegalStateException("找不到内置 handler: " + req.getHandlerValue());
                }
                msg = h.execute(req.getInstanceId(), req.getParam());
            }
            status = JobInstance.SUCCESS;
            statSuccess.incrementAndGet();
        } catch (Exception e) {
            status = JobInstance.FAILED;
            msg = e.getClass().getSimpleName() + ": " + e.getMessage();
            statFailed.incrementAndGet();
            log.warn("任务执行失败 instance={} err={}", req.getInstanceId(), msg);
        }
        report(req.getCallbackUrl(), req.getInstanceId(), status, msg,
                System.currentTimeMillis() - start, req.getRetryCount());
    }

    /** HTTP 回调型任务：把派发请求原样转发给业务 URL。 */
    private String callHttpHandler(String url, DispatchRequest req) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("instanceId", req.getInstanceId());
        body.put("jobId", req.getJobId());
        body.put("jobName", req.getJobName());
        String resp = client.post().uri(url)
                .header("Content-Type", "application/json")
                .body(body)
                .retrieve()
                .body(String.class);
        return "HTTP 回调返回: " + resp;
    }

    /** 把执行结果回调给调度中心。回调失败只记日志 —— 超时后会被故障转移兜住。 */
    private void report(String callbackUrl, Long instanceId, int status, String msg,
                        long elapsedMs, Integer retryCount) {
        if (callbackUrl == null || callbackUrl.isBlank()) {
            log.warn("没有回调地址，结果丢弃 instance={}", instanceId);
            return;
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("instanceId", instanceId);
        body.put("status", status);
        body.put("message", msg == null ? "" : msg.substring(0, Math.min(msg.length(), 900)));
        body.put("elapsedMs", elapsedMs);
        // ★ 回传 retryCount：中心靠它识别「这是哪一次尝试的回调」，
        //   从而丢弃迟到的旧回调，避免重试次数被重复累加
        body.put("retryCount", retryCount == null ? 0 : retryCount);
        try {
            client.post().uri(callbackUrl)
                    .header("Content-Type", "application/json")
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
        } catch (Exception e) {
            log.error("回调失败 instance={} url={} err={}", instanceId, callbackUrl, e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        pool.shutdown();
    }
}
