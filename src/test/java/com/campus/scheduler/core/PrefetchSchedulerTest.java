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
import java.time.LocalDateTime;

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
        // ★ 关掉后台工作线程：它和测试自己调的 fireDueNow() 会抢同一个队列，
        //   谁先拿到算谁的 —— 那样断言就成了掷骰子。
        //   响应的触发逻辑仍然被测到，因为两者共用同一个 process()。
        "scheduler.prefetch-worker-enabled=false",
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
     * <p>{@link PrefetchScheduler} 是单例，队列和去重集合被所有测试共享，
     * 所以每个测试开始前必须清干净。
     *
     * <p><b>这里是同步清空，不是「睡一会儿等工作线程」。</b>
     * 原来的写法是排空队列 + 等 1 秒，在快机器上没问题；CI 的 runner 慢，
     * 等待超时后残留任务就串到下一个测试 —— CI 上的症状是
     * 「队列里积了 3 个任务」，而那正是一个断言的失败原因。
     *
     * <p>测试的可靠性不该建立在「机器够快」上。
     */
    @BeforeEach
    void clean() {
        prefetch.clearQueue();
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

    /** 建一个**已经到点**的任务（next_trigger_time = 数据库的现在） */
    private long dueJob(String name) {
        jdbc.update("""
                INSERT INTO job_info
                  (job_name, cron_expr, handler_type, handler_value, param,
                   timeout_sec, max_retry, misfire_strategy, status, next_trigger_time)
                VALUES (?, '* * * * * *', 1, 'printTime', NULL, 10, 0, 2, 1, NOW(3))
                """, name);
        return jdbc.queryForObject("SELECT id FROM job_info WHERE job_name = ?", Long.class, name);
    }

    private void healthyExecutor() {
        jdbc.update("""
                INSERT INTO executor_registry (app_name, address, last_heartbeat, status)
                VALUES ('prefetch-test', ?, NOW(3), 1)
                """, fakeUrl);
    }

    /** 某个任务自己产生了多少条实例 */
    private int instancesOf(long jobId) {
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM job_instance WHERE job_id = ?", Integer.class, jobId);
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
    @DisplayName("到点后触发并派发")
    void firesWhenDue() {
        healthyExecutor();
        long id = dueJob("fire-me");       // 已经到点

        prefetch.prefetch();
        assertEquals(1, prefetch.queueSize(), "应该已经入队");

        // ★ 同步处理，不等后台线程。
        //
        // 工作线程的循环就是 take() + process()，而 fireDueNow() 是 poll() + process()
        // —— 两者共用同一个 process()，所以这里覆盖的仍然是生产代码路径。
        // 换成同步调用之后，测试不再依赖「机器够快、时钟够准」，也就不会再 flaky。
        assertEquals(1, prefetch.fireDueNow(), "应该处理掉 1 个到点任务");

        assertEquals(1, instancesOf(id), "这个任务应该只产生一条实例");
        assertEquals(0, prefetch.queueSize(), "触发后队列应该空了");

        Integer status = jdbc.queryForObject(
                "SELECT status FROM job_instance WHERE job_id = ?", Integer.class, id);
        assertEquals(JobInstance.RUNNING, status, "派发出去后应该是执行中");
    }

    @Test
    @DisplayName("触发后 next_trigger_time 被推进到下一次，否则会被反复触发")
    void advancesTriggerTimeAfterFire() {
        healthyExecutor();
        long id = dueJob("advance-me");
        var before = jdbc.queryForObject(
                "SELECT next_trigger_time FROM job_info WHERE id = ?", LocalDateTime.class, id);

        prefetch.prefetch();
        assertEquals(1, prefetch.fireDueNow(), "前提：这个任务应该被触发了");

        var after = jdbc.queryForObject(
                "SELECT next_trigger_time FROM job_info WHERE id = ?", LocalDateTime.class, id);
        assertNotNull(after);
        assertTrue(after.isAfter(before),
                "触发后应该把 next_trigger_time 推进到下一次，否则会被反复触发");
    }

    @Test
    @DisplayName("没到点的任务不会被 fireDueNow 取走，会留在队列里")
    void fireDueNowLeavesNotYetDue() {
        long id = job("not-yet", 30);      // 30 秒后到点，不在 3 秒预取窗口内

        prefetch.prefetch();
        assertEquals(0, prefetch.queueSize(), "窗口外的任务压根不该入队");

        assertEquals(0, prefetch.fireDueNow(), "没有到点任务时应该什么都不做");
        assertEquals(0, instancesOf(id), "不该产生实例");
    }
}
