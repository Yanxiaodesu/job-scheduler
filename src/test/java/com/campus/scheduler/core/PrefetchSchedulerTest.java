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

import static org.junit.jupiter.api.Assertions.*;

/**
 * 内存延迟队列（预取）调度的测试。
 *
 * <h2>为什么要单独测这一块</h2>
 *
 * <p>预取调度是后来加的优化（精度 108ms → 16ms），它引入了一份
 * <b>易失的内存状态</b>（DelayQueue + 去重集合）。易失状态最容易出两类问题：
 *
 * <ul>
 *   <li><b>重复入队</b>：预取每 1 秒查一次「未来 3 秒到点」的任务，
 *       同一个任务会被连续查到 2~3 次。去重集合失效就会重复触发</li>
 *   <li><b>漏任务</b>：窗口设小了、或者预取异常，任务就永远不会被触发</li>
 * </ul>
 *
 * <p>{@code prefetch-interval-ms} 设成 1 小时 = 关掉自动预取，
 * 由测试手动调 {@link PrefetchScheduler#prefetch()}，保证确定性。
 */
@SpringBootTest(properties = {
        "scheduler.role=admin",
        "scheduler.scan-interval-ms=3600000",
        "scheduler.failover-interval-ms=3600000",
        "scheduler.retry-interval-ms=3600000",
        "scheduler.prefetch-enabled=true",
        // 关掉自动预取（否则会和测试自己的调用抢），手动调
        "scheduler.prefetch-interval-ms=3600000",
        "scheduler.prefetch-window-sec=3",
        "scheduler.executor.app-name=prefetch-test"
})
class PrefetchSchedulerTest {

    @Autowired
    private PrefetchScheduler prefetch;
    @Autowired
    private JdbcTemplate jdbc;

    private static HttpServer fakeExecutor;
    private static String fakeUrl;

    @BeforeAll
    static void startFakeExecutor() throws IOException {
        fakeExecutor = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeExecutor.createContext("/executor/run", ex -> {
            byte[] body = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
            ex.close();
        });
        fakeExecutor.start();
        fakeUrl = "http://127.0.0.1:" + fakeExecutor.getAddress().getPort();
    }

    @AfterAll
    static void stopFakeExecutor() {
        if (fakeExecutor != null) {
            fakeExecutor.stop(0);
        }
    }

    /**
     * 清场。
     *
     * <p><b>关键是要先排空队列</b>：{@link PrefetchScheduler} 是单例，
     * 它的 {@code DelayQueue} 和去重集合会被所有测试共享。上一个测试
     * 留在队列里的任务，会被工作线程消费掉并真的触发一次 —— 于是
     * 下一个测试就会看到「凭空多出来一条实例」和「队列数对不上」。
     *
     * <p>（这个坑我踩了：最初的清理只删表，没排空队列，两个测试因此失败。）
     */
    @BeforeEach
    void clean() throws Exception {
        // 队列里的任务会被工作线程消费掉。等它排空 ——
        // 预取窗口内最远 3 秒，留 6 秒足够
        long deadline = System.currentTimeMillis() + 6000;
        while (prefetch.queueSize() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        jdbc.update("DELETE FROM job_instance");
        jdbc.update("DELETE FROM job_log");
        jdbc.update("DELETE FROM executor_registry");
        jdbc.update("DELETE FROM job_info");
    }

    /** 建一个任务，next_trigger_time 设为「未来 seconds 秒」 */
    private long job(String name, int secondsAhead) {
        jdbc.update("""
                INSERT INTO job_info
                  (job_name, cron_expr, handler_type, handler_value, param,
                   timeout_sec, max_retry, misfire_strategy, status, next_trigger_time)
                VALUES (?, '* * * * * *', 1, 'printTime', NULL, 10, 0, 2, 1,
                        NOW(3) + INTERVAL ? SECOND)
                """, name, secondsAhead);
        return jdbc.queryForObject("SELECT id FROM job_info WHERE job_name = ?", Long.class, name);
    }

    private void healthyExecutor() {
        jdbc.update("""
                INSERT INTO executor_registry (app_name, address, last_heartbeat, status)
                VALUES ('prefetch-test', ?, NOW(3), 1)
                """, fakeUrl);
    }

    private int instances() {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM job_instance", Integer.class);
        return n == null ? 0 : n;
    }

    @Test
    @DisplayName("预取：窗口内即将到点的任务被装进内存队列")
    void prefetchLoadsUpcomingJobs() {
        job("soon", 2);          // 2 秒后到点，在 3 秒窗口内
        int before = prefetch.queueSize();

        prefetch.prefetch();

        assertEquals(before + 1, prefetch.queueSize(), "窗口内的任务应该被装进队列");
    }

    @Test
    @DisplayName("预取：窗口外的远期任务不会被装进队列")
    void prefetchSkipsFarFuture() {
        job("far", 60);          // 60 秒后，远超 3 秒窗口
        int before = prefetch.queueSize();

        prefetch.prefetch();

        assertEquals(before, prefetch.queueSize(), "窗口外的任务不该被预取");
    }

    @Test
    @DisplayName("预取：同一个任务反复预取也只入队一次（去重生效）")
    void prefetchDeduplicates() {
        job("dup", 2);
        int before = prefetch.queueSize();

        // 模拟预取线程连续跑 5 轮
        for (int i = 0; i < 5; i++) {
            prefetch.prefetch();
        }

        assertEquals(before + 1, prefetch.queueSize(),
                "预取每 1 秒查一次、窗口 3 秒，同一任务必然被查到多次 —— 去重必须生效");
    }

    @Test
    @DisplayName("预取：已停用的任务不会被装进队列")
    void prefetchSkipsDisabledJobs() {
        long id = job("disabled", 2);
        jdbc.update("UPDATE job_info SET status = 0 WHERE id = ?", id);
        int before = prefetch.queueSize();

        prefetch.prefetch();

        assertEquals(before, prefetch.queueSize(), "停用的任务不该被预取");
    }

    @Test
    @DisplayName("到点后工作线程会自动触发并派发")
    void firesWhenDue() throws Exception {
        healthyExecutor();
        job("fire-me", 1);       // 1 秒后到点

        prefetch.prefetch();
        assertEquals(1, prefetch.queueSize(), "应该已经入队");

        // 工作线程会在到点时醒来。给它留够时间
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline && instances() == 0) {
            Thread.sleep(100);
        }

        assertEquals(1, instances(), "到点后应该自动产生一条实例并派发出去");
        assertEquals(0, prefetch.queueSize(), "触发后队列应该空了");

        Integer status = jdbc.queryForObject(
                "SELECT status FROM job_instance", Integer.class);
        assertEquals(JobInstance.RUNNING, status, "派发出去后应该是执行中");
    }

    @Test
    @DisplayName("触发后 next_trigger_time 被推进到下一次，否则会被反复触发")
    void advancesTriggerTimeAfterFire() throws Exception {
        healthyExecutor();
        long id = job("advance-me", 1);
        var before = jdbc.queryForObject(
                "SELECT next_trigger_time FROM job_info WHERE id = ?", java.sql.Timestamp.class, id);

        prefetch.prefetch();
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline && instances() == 0) {
            Thread.sleep(100);
        }
        assertEquals(1, instances(), "前提：任务应该被触发了");

        var after = jdbc.queryForObject(
                "SELECT next_trigger_time FROM job_info WHERE id = ?", java.sql.Timestamp.class, id);
        assertNotNull(after);
        assertTrue(after.after(before),
                "触发后应该把 next_trigger_time 推进到下一次，否则会被反复触发");
    }
}
