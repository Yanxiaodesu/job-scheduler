package com.campus.scheduler.core;

import com.campus.scheduler.model.JobInstance;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 故障转移与 misfire 的测试。
 *
 * <h2>为什么这些测试重要</h2>
 *
 * <p>调度循环走的是「正常路径」，而一个调度系统真正的价值在于
 * <b>异常路径</b>：执行器挂了、任务卡住了、中心停机错过了触发。
 * 这些路径很难靠手工点几下复现，必须用测试固定住。
 *
 * <h2>测试手法</h2>
 *
 * <p>把 {@code @Scheduled} 的间隔全部设成 1 小时（等于关掉自动触发），
 * 然后<b>直接调用</b> {@link FailoverTask#run()} / {@link FailoverTask#retryPending()}。
 * 这样测试是确定性的，不依赖时序。
 */
@SpringBootTest(properties = {
        "scheduler.role=admin",
        "scheduler.scan-interval-ms=3600000",
        "scheduler.failover-interval-ms=3600000",
        "scheduler.retry-interval-ms=3600000",
        "scheduler.misfire-threshold-sec=60",
        "scheduler.executor.app-name=test-app",
        "scheduler.executor.offline-threshold-sec=30"
})
class FailoverTaskTest {

    @Autowired
    private FailoverTask failover;
    @Autowired
    private JdbcTemplate jdbc;

    /**
     * 三个「假执行器」，用 JDK 自带的 HttpServer 起在随机端口上。
     *
     * <p>为什么要真的起 HTTP 服务而不是随便写个地址：派发失败时
     * {@code InstanceDispatcher} 会把实例退回 PENDING（这是个重要的正确性行为）。
     * 如果地址是死的，测试就只能验证「失败路径」，验证不了转移/补跑到底有没有发出去。
     */
    private static final List<HttpServer> SERVERS = new ArrayList<>();
    private static final List<String> FAKE = new ArrayList<>();

    @BeforeAll
    static void startFakeExecutors() throws IOException {
        for (int i = 0; i < 3; i++) {
            HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            s.createContext("/executor/run", ex -> {
                byte[] body = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().add("Content-Type", "application/json");
                ex.sendResponseHeaders(200, body.length);
                ex.getResponseBody().write(body);
                ex.close();
            });
            s.start();
            SERVERS.add(s);
            FAKE.add("http://127.0.0.1:" + s.getAddress().getPort());
        }
    }

    @AfterAll
    static void stopFakeExecutors() {
        SERVERS.forEach(s -> s.stop(0));
    }

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM job_instance");
        jdbc.update("DELETE FROM job_log");
        jdbc.update("DELETE FROM executor_registry");
        jdbc.update("DELETE FROM job_info");
    }

    // ==================== 测试数据构造 ====================

    /** 建任务。misfireStrategy: 1=补跑一次, 2=跳过 */
    private long job(String name, int misfireStrategy, int maxRetry, int timeoutSec) {
        jdbc.update("""
                INSERT INTO job_info
                  (job_name, cron_expr, handler_type, handler_value, param,
                   timeout_sec, max_retry, misfire_strategy, status, next_trigger_time)
                VALUES (?, '0 0 0 1 1 *', 1, 'printTime', NULL, ?, ?, ?, 1, NULL)
                """, name, timeoutSec, maxRetry, misfireStrategy);
        return jdbc.queryForObject("SELECT id FROM job_info WHERE job_name = ?", Long.class, name);
    }

    /** 建一个实例。triggerAgoSec 表示触发时刻是多久以前 */
    private long instance(long jobId, int status, int retryCount, int triggerAgoSec) {
        String key = jobId + ":t" + System.nanoTime();
        jdbc.update("""
                INSERT INTO job_instance (job_id, trigger_time, instance_key, status, retry_count, create_time)
                VALUES (?, NOW(3) - INTERVAL ? SECOND, ?, ?, ?, NOW(3) - INTERVAL ? SECOND)
                """, jobId, triggerAgoSec, key, status, retryCount, triggerAgoSec);
        return jdbc.queryForObject(
                "SELECT id FROM job_instance WHERE instance_key = ?", Long.class, key);
    }

    private void markStart(long instanceId, int agoSec) {
        jdbc.update("UPDATE job_instance SET start_time = NOW(3) - INTERVAL ? SECOND WHERE id = ?",
                agoSec, instanceId);
    }

    private long executor(String appName, String address, int heartbeatAgoSec, int status) {
        jdbc.update("""
                INSERT INTO executor_registry (app_name, address, last_heartbeat, status)
                VALUES (?, ?, NOW(3) - INTERVAL ? SECOND, ?)
                """, appName, address, heartbeatAgoSec, status);
        return jdbc.queryForObject(
                "SELECT id FROM executor_registry WHERE app_name=? AND address=?", Long.class, appName, address);
    }

    private Map<String, Object> load(long instanceId) {
        return jdbc.queryForMap("""
                SELECT status, retry_count, executor_id, result_msg FROM job_instance WHERE id = ?
                """, instanceId);
    }

    // ==================== misfire ====================

    @Test
    @DisplayName("misfire 跳过策略：错过很久的实例被标记为「已跳过」")
    void misfire_skip() {
        long j = job("misfire-skip", 2, 0, 30);
        // 触发时刻在 5 分钟前（远超 misfire 阈值 60 秒），还是 PENDING
        long inst = instance(j, JobInstance.PENDING, 0, 300);

        failover.run();

        Map<String, Object> row = load(inst);
        assertEquals(JobInstance.SKIPPED, row.get("status"), "跳过策略应该把实例标记为已跳过");
        assertTrue(String.valueOf(row.get("result_msg")).contains("misfire"),
                "结果里应该写明是 misfire 导致的跳过");
    }

    @Test
    @DisplayName("misfire 补跑策略：错过的实例被重新派发（状态变执行中）")
    void misfire_rerun() {
        long j = job("misfire-rerun", 1, 0, 30);
        long inst = instance(j, JobInstance.PENDING, 0, 300);
        // 有一个健康执行器可派
        executor("test-app", FAKE.get(0), 0, 1);

        failover.run();

        Map<String, Object> row = load(inst);
        assertEquals(JobInstance.RUNNING, row.get("status"),
                "补跑策略应该把实例派发出去（状态推进到执行中）");
        assertNotNull(row.get("executor_id"), "应该已经分配给某个执行器");
    }

    @Test
    @DisplayName("刚触发的实例不该被当成 misfire")
    void freshInstanceIsNotMisfire() {
        long j = job("fresh", 2, 0, 30);
        long inst = instance(j, JobInstance.PENDING, 0, 5);   // 5 秒前触发，阈值是 60 秒

        failover.run();

        assertEquals(JobInstance.PENDING, load(inst).get("status"),
                "没超过阈值的实例不能被误判为 misfire");
    }

    // ==================== 超时看门狗 ====================

    @Test
    @DisplayName("超时看门狗：执行中但超时的实例被标记为「超时」")
    void timeout_noRetryLeft() {
        long j = job("timeout-noretry", 2, 0, 10);     // maxRetry=0
        long inst = instance(j, JobInstance.RUNNING, 0, 0);
        markStart(inst, 100);                          // 100 秒前开始执行，超时阈值 10+10=20 秒

        failover.run();

        assertEquals(JobInstance.TIMEOUT, load(inst).get("status"));
    }

    @Test
    @DisplayName("超时看门狗：还有重试次数时退回待执行，等重派")
    void timeout_withRetryLeft() {
        long j = job("timeout-retry", 2, 2, 10);       // maxRetry=2
        long inst = instance(j, JobInstance.RUNNING, 0, 0);
        markStart(inst, 100);

        failover.run();

        Map<String, Object> row = load(inst);
        assertEquals(JobInstance.PENDING, row.get("status"), "还有重试次数时应该退回待执行");
        assertNull(row.get("executor_id"), "退回后要清空执行器，等待重新分配");
    }

    @Test
    @DisplayName("还没超时的执行中实例不该被误杀")
    void runningNotYetTimeout() {
        long j = job("running-ok", 2, 0, 60);          // 超时 60+10=70 秒
        long inst = instance(j, JobInstance.RUNNING, 0, 0);
        markStart(inst, 10);                            // 才跑了 10 秒

        failover.run();

        assertEquals(JobInstance.RUNNING, load(inst).get("status"), "没超时的不能动");
    }

    // ==================== 离线判定与转移 ====================

    @Test
    @DisplayName("心跳超时的执行器被标记为离线")
    void offlineDetection() {
        executor("test-app", "http://127.0.0.1:19998", 120, 1);   // 心跳停在 120 秒前，阈值 30 秒

        failover.run();

        Integer status = jdbc.queryForObject(
                "SELECT status FROM executor_registry WHERE address = 'http://127.0.0.1:19998'",
                Integer.class);
        assertEquals(0, status, "心跳超时应该被判离线");
    }

    @Test
    @DisplayName("心跳正常的执行器保持在线")
    void healthyExecutorStaysOnline() {
        executor("test-app", "http://127.0.0.1:19997", 2, 1);

        failover.run();

        Integer status = jdbc.queryForObject(
                "SELECT status FROM executor_registry WHERE address = 'http://127.0.0.1:19997'",
                Integer.class);
        assertEquals(1, status, "心跳正常不能被误判离线");
    }

    @Test
    @DisplayName("卡在已离线执行器上的实例被转移")
    void transferFromOfflineExecutor() {
        long j = job("transfer", 2, 0, 300);
        long dead = executor("test-app", "http://127.0.0.1:19996", 300, 1);   // 会先被判离线

        long inst = instance(j, JobInstance.RUNNING, 0, 0);
        jdbc.update("UPDATE job_instance SET executor_id = ?, start_time = NOW(3) WHERE id = ?",
                dead, inst);

        // 另有一个健康执行器作为转移目标
        long alive = executor("test-app", FAKE.get(1), 0, 1);

        failover.run();

        Map<String, Object> row = load(inst);
        assertEquals(alive, ((Number) row.get("executor_id")).longValue(),
                "应该被转移到健康的执行器上");
        assertEquals(JobInstance.RUNNING, row.get("status"));
        assertEquals(0, row.get("retry_count"),
                "转移不算一次失败尝试，retry_count 不应增加");
    }

    @Test
    @DisplayName("已成功/已失败的历史实例绝不能被重跑")
    void finishedInstancesAreNeverRetouched() {
        long j = job("finished", 1, 0, 10);
        long ok = instance(j, JobInstance.SUCCESS, 0, 500);
        long bad = instance(j, JobInstance.FAILED, 0, 500);
        long timedOut = instance(j, JobInstance.TIMEOUT, 0, 500);
        executor("test-app", "http://127.0.0.1:19994", 0, 1);

        failover.run();
        failover.retryPending();

        assertEquals(JobInstance.SUCCESS, load(ok).get("status"), "成功的绝不能重跑");
        assertEquals(JobInstance.FAILED, load(bad).get("status"), "失败的绝不能重跑");
        assertEquals(JobInstance.TIMEOUT, load(timedOut).get("status"), "超时的绝不能重跑");
    }

    // ==================== 重新派发 ====================

    @Test
    @DisplayName("重派：待执行且未分配的实例会被重新派发出去")
    void retryPendingDispatches() {
        long j = job("repend", 2, 2, 30);
        // 触发时刻是 30 秒前，建的时候也是 30 秒前 → 超过 5 秒宽限期
        long inst = instance(j, JobInstance.PENDING, 1, 30);
        executor("test-app", FAKE.get(2), 0, 1);

        failover.retryPending();

        Map<String, Object> row = load(inst);
        assertEquals(JobInstance.RUNNING, row.get("status"), "应该被派发出去");
        assertNotNull(row.get("executor_id"));
    }

    @Test
    @DisplayName("重派宽容期：刚建的实例不会被抢着重派（避免和正常路径打架）")
    void retryPendingRespectsGracePeriod() {
        long j = job("grace", 2, 2, 30);
        long inst = instance(j, JobInstance.PENDING, 0, 0);    // 刚刚才建
        executor("test-app", "http://127.0.0.1:19992", 0, 1);

        failover.retryPending();

        assertEquals(JobInstance.PENDING, load(inst).get("status"),
                "宽限期内的实例应该留给正常路径，重派不能插手");
    }

    @Test
    @DisplayName("重派：没有健康执行器时保持原状，不丢实例")
    void retryPendingWithoutExecutor() {
        long j = job("noexec", 2, 2, 30);
        long inst = instance(j, JobInstance.PENDING, 0, 30);
        // 故意不建任何执行器

        failover.retryPending();

        assertEquals(JobInstance.PENDING, load(inst).get("status"), "没有执行器时应该原样保留");
    }
}
