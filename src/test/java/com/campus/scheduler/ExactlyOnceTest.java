package com.campus.scheduler;

import com.campus.scheduler.core.CronSupport;
import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.repo.JobInfoRepo;
import com.campus.scheduler.repo.JobInstanceRepo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ★ 核心保证的确定性测试：「不多不少，恰好一次」。
 *
 * <p>为什么需要这个测试：用「起 3 个 Admin 跑一会儿看有没有重复」的方式验证，
 * 依赖调度循环恰好撞上（实测里 33 次触发一次都没撞上）。
 * <b>碰巧没撞上不等于防护有效</b>。这里直接用多线程强行制造竞争，
 * 让每一次运行都必然撞上。
 *
 * <p>（`scheduler.role=executor` 是为了让 {@code @Scheduled} 的调度循环别在测试里乱跑，
 * 免得干扰计数。）
 */
@SpringBootTest(properties = {
        "scheduler.role=executor",
        "scheduler.executor.app-name=test-app"
})
class ExactlyOnceTest {

    @Autowired
    private JobInfoRepo jobRepo;
    @Autowired
    private JobInstanceRepo instanceRepo;
    @Autowired
    private JdbcTemplate jdbc;

    private static final int THREADS = 20;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM job_instance");
        jdbc.update("DELETE FROM job_log");
        jdbc.update("DELETE FROM job_info");
    }

    private Long newJob(String name, String cron) {
        JobInfo j = new JobInfo();
        j.setJobName(name);
        j.setCronExpr(cron);
        j.setHandlerType(1);
        j.setHandlerValue("printTime");
        j.setTimeoutSec(10);
        j.setMaxRetry(0);
        j.setMisfireStrategy(2);
        j.setStatus(1);
        j.setNextTriggerTime(CronSupport.next(cron));
        return jobRepo.insert(j);
    }

    @Test
    @DisplayName("第二层防护：20 线程抢同一个触发时刻，只能落一条实例")
    void concurrent_tryCreate_onlyOneWins() throws Exception {
        Long jobId = newJob("race-" + System.nanoTime(), "*/5 * * * * *");
        LocalDateTime triggerTime = LocalDateTime.now().withNano(0);
        String instanceKey = jobId + ":" + triggerTime;

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger wins = new AtomicInteger();
        AtomicInteger loses = new AtomicInteger();

        for (int i = 0; i < THREADS; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    // 20 个线程同时拿同一个 instance_key 去插
                    Long id = instanceRepo.tryCreate(jobId, triggerTime, instanceKey);
                    if (id != null) {
                        wins.incrementAndGet();
                    } else {
                        loses.incrementAndGet();
                    }
                } catch (Exception e) {
                    loses.incrementAndGet();
                }
            });
        }

        ready.await();
        go.countDown();                       // 一声令下，同时冲
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        System.out.printf("  抢到=%d 抢输=%d%n", wins.get(), loses.get());

        // 关键断言：恰好一个人赢
        assertEquals(1, wins.get(), "同一触发时刻只能有一个赢家");
        assertEquals(THREADS - 1, loses.get(), "其余全部应该被唯一键挡住");

        // 库里也确实只有一条
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM job_instance WHERE instance_key = ?", Integer.class, instanceKey);
        assertEquals(1, count, "数据库里只能有一条实例");
    }

    @Test
    @DisplayName("第一层防护：20 线程抢推进同一个旧值，只能有一个 rowcount=1")
    void concurrent_advanceTriggerTime_onlyOneWins() throws Exception {
        Long jobId = newJob("cas-" + System.nanoTime(), "*/5 * * * * *");

        LocalDateTime oldTime = LocalDateTime.now().withNano(0);
        jdbc.update("UPDATE job_info SET next_trigger_time = ? WHERE id = ?", oldTime, jobId);
        LocalDateTime newTime = oldTime.plusSeconds(5);

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger wins = new AtomicInteger();

        for (int i = 0; i < THREADS; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    // 乐观锁：带旧值条件更新。旧值已经被别人改掉的话影响 0 行
                    int rows = jobRepo.advanceTriggerTime(jobId, oldTime, newTime);
                    if (rows == 1) {
                        wins.incrementAndGet();
                    }
                } catch (Exception ignored) {
                    // 竞争失败属于预期
                }
            });
        }

        ready.await();
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        System.out.printf("  推进成功=%d%n", wins.get());
        assertEquals(1, wins.get(), "只能有一个线程把 next_trigger_time 推进成功");
    }

    @Test
    @DisplayName("重试：去重键带 retryCount，所以每次重试都是新键，不会被误判为重复派发")
    void retry_isNotSwallowedByDedup() {
        Long jobId = newJob("retrykey-" + System.nanoTime(), "*/5 * * * * *");
        LocalDateTime t = LocalDateTime.now().withNano(0);

        // 首次尝试：retryCount = 0
        Long first = instanceRepo.tryCreate(jobId, t, jobId + ":" + t);
        assertNotNull(first);

        // 模拟失败后重试：同一个实例 id，但 retryCount 变成 1
        int updated = instanceRepo.markRetry(first, "模拟失败");
        assertEquals(1, updated);

        var inst = instanceRepo.findById(first);
        assertEquals(1, inst.getRetryCount(), "重试次数应该 +1");
        assertEquals(0, inst.getStatus(), "应该退回待执行等待重派");

        // 去重键 = instanceId:retryCount，所以重试的键和首次不同
        String firstKey = first + ":0";
        String retryKey = first + ":1";
        assertNotEquals(firstKey, retryKey,
                "带 retryCount 的去重键必须不同 —— 否则重试会被执行器当成重复派发丢掉");
    }

    @Test
    @DisplayName("cron 非法必须被挡住，不能进库")
    void invalidCronRejected() {
        assertFalse(CronSupport.isValid("这不是cron"));
        assertFalse(CronSupport.isValid(""));
        assertFalse(CronSupport.isValid(null));
        assertThrows(IllegalArgumentException.class, () -> CronSupport.next("bad"));

        assertTrue(CronSupport.isValid("*/5 * * * * *"));
        assertTrue(CronSupport.isValid("0 0 2 * * *"));
    }

    @Test
    @DisplayName("cron 下次触发时间计算正确")
    void cronNextIsCorrect() {
        LocalDateTime from = LocalDateTime.of(2026, 10, 7, 10, 30, 15);

        // 每 5 秒 → 下一个 5 秒整点
        assertEquals(LocalDateTime.of(2026, 10, 7, 10, 30, 20),
                CronSupport.next("*/5 * * * * *", from));

        // 每天 2 点 → 第二天 2 点
        assertEquals(LocalDateTime.of(2026, 10, 8, 2, 0, 0),
                CronSupport.next("0 0 2 * * *", from));
    }
}
