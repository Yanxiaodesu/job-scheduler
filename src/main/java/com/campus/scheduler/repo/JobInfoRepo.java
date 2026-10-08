package com.campus.scheduler.repo;

import com.campus.scheduler.model.JobInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class JobInfoRepo {

    private final JdbcTemplate jdbc;

    public static final RowMapper<JobInfo> MAPPER = (rs, i) -> {
        JobInfo j = new JobInfo();
        j.setId(rs.getLong("id"));
        j.setJobName(rs.getString("job_name"));
        j.setCronExpr(rs.getString("cron_expr"));
        j.setHandlerType(rs.getInt("handler_type"));
        j.setHandlerValue(rs.getString("handler_value"));
        j.setParam(rs.getString("param"));
        j.setTimeoutSec(rs.getInt("timeout_sec"));
        j.setMaxRetry(rs.getInt("max_retry"));
        j.setMisfireStrategy(rs.getInt("misfire_strategy"));
        j.setStatus(rs.getInt("status"));
        // ★ 时间列一律用 getObject(..., LocalDateTime.class) 读。
        //
        // 不要写 rs.getTimestamp(...).toLocalDateTime()：Timestamp 表示一个「时刻」，
        // 驱动会按「JVM 时区 ↔ serverTimezone」把它转换一遍。而 DATETIME 列
        // 本身**没有时区概念**，这个转换纯属多余，还会让「读到的值」和
        // 「写进去的值」对不上。
        //
        // 这个坑极隐蔽：开发机的 JVM 时区恰好和 JDBC URL 里的 serverTimezone
        // 一致（都是 +08:00），转换退化成恒等变换，怎么试都没问题；
        // 一到 UTC 的机器上（CI runner、或者没设 TZ 的容器）就整体偏 8 小时。
        j.setNextTriggerTime(rs.getObject("next_trigger_time", LocalDateTime.class));
        j.setCreateTime(rs.getObject("create_time", LocalDateTime.class));
        j.setUpdateTime(rs.getObject("update_time", LocalDateTime.class));
        return j;
    };

    /**
     * ★★ 调度循环的入口：捞出「已经到点」的任务。
     *
     * <p>靠 {@code idx_due (status, next_trigger_time)} 走索引，
     * 不然任务一多就退化成全表扫描。
     *
     * <p>加了 LIMIT：一次扫太多会把调度循环卡住。剩下的下一轮再处理，
     * 反正下一秒还会扫。
     */
    public List<JobInfo> findDue(int limit) {
        return jdbc.query("""
                SELECT * FROM job_info
                 WHERE status = 1
                   AND next_trigger_time IS NOT NULL
                   AND next_trigger_time <= NOW()
                 ORDER BY next_trigger_time
                 LIMIT ?
                """, MAPPER, limit);
    }

    /**
     * 预取结果：任务 + **由数据库算出的**「距离到点还有多少微秒」。
     *
     * <p><b>为什么不让 Java 自己算</b>：那需要拿数据库的墙上时间
     * （{@code next_trigger_time}）去套 JVM 的时区（{@code ZoneId.systemDefault()}）。
     * 只要两边时区不一致，算出来的「到点时刻」就是错的 —— 任务永远不到点，
     * {@code DelayQueue.take()} 一直阻塞，队列越积越多（CI 上就是这么挂的：
     * 两个测试失败，其中一个的症状是「队列里积了 3 个任务」）。
     *
     * <p>让数据库用 {@code TIMESTAMPDIFF} 算「它自己的 NOW() 到 next_trigger_time
     * 差多久」，时区就完全无关了：JVM 只负责把这个**相对时长**锚定到自己的时钟上。
     */
    public record Upcoming(JobInfo job, long dueInMicros) {
    }

    /**
     * 数据库的当前时间。
     *
     * <p><b>算「下一次触发时刻」必须用它，不能用 {@code LocalDateTime.now()}。</b>
     *
     * <p>原因：{@code next_trigger_time} 是数据库的时钟写的，如果拿 JVM 的时钟去算
     * 「下一次」，两边时区或时钟不一致时算出来的时刻可能落在**过去** ——
     * 任务会被反复触发。CI 上就是这么暴露的（一个断言报
     * 「触发后应该把 next_trigger_time 推进到下一次」，因为推进后的值反而更早）。
     *
     * <p>和 {@code JobInfoRepo.Upcoming} 是同一类修法：**凡是和时间有关的计算，
     * 都以数据库的时钟为准**，JVM 只负责相对量。
     */
    public LocalDateTime now() {
        return jdbc.queryForObject("SELECT NOW(3)", LocalDateTime.class);
    }

    /**
     * ★ 预取用：捞出「未来 N 秒内将要到点」的任务，提前装进内存。
     *
     * <p>这是 {@link com.campus.scheduler.core.PrefetchScheduler} 的基础。
     * 和 {@link #findDue} 的区别：那个只看「已经到点」的，这个看「即将到点」的。
     *
     * <p>为什么需要预取：如果每次都查「已经到点」的，那么从「到点」到
     * 「发现它到点」之间的延迟就不可避免地等于扫描间隔。提前把任务装进内存，
     * 到点那一刻可以直接触发，精度只受限于内存定时器的唤醒精度。
     */
    public List<Upcoming> findUpcoming(int withinSec, int limit) {
        return jdbc.query("""
                SELECT j.*, TIMESTAMPDIFF(MICROSECOND, NOW(3), j.next_trigger_time) AS due_in_us
                  FROM job_info j
                 WHERE j.status = 1
                   AND j.next_trigger_time IS NOT NULL
                   AND j.next_trigger_time <= NOW(3) + INTERVAL ? SECOND
                 ORDER BY j.next_trigger_time
                 LIMIT ?
                """,
                (rs, i) -> new Upcoming(MAPPER.mapRow(rs, i), rs.getLong("due_in_us")),
                withinSec, limit);
    }

    /**
     * ★★ 抢占这次触发 —— 整个项目最关键的一行 SQL。
     *
     * <pre>
     * UPDATE job_info SET next_trigger_time = 下次
     *  WHERE id = ? AND next_trigger_time = 旧值
     * </pre>
     *
     * <p>为什么这样就够了：多个 Admin 实例同时扫到同一个到点任务时，
     * 它们手里的「旧值」是同一个时刻。第一个 UPDATE 成功后，
     * 第二个的 WHERE 条件不再成立 → 影响 0 行 → 它就知道自己没抢到，直接跳过。
     *
     * <p><b>这正是乐观锁</b>（CAS 思想）：不加锁、不阻塞，用「比较并交换」保证只有一个赢。
     * 好处是少一个 Redis 组件；代价是高并发时失败的一方要重试，
     * 但调度场景下同一任务被同时抢的概率很低，完全划算。
     *
     * @return 影响行数。**1 才代表抢到了**，0 说明别人先动手了
     */
    public int advanceTriggerTime(Long id, LocalDateTime expectedOld, LocalDateTime next) {
        return jdbc.update(
                "UPDATE job_info SET next_trigger_time = ? WHERE id = ? AND next_trigger_time = ?",
                next, id, expectedOld);
    }

    public List<JobInfo> findAll() {
        return jdbc.query("SELECT * FROM job_info ORDER BY id", MAPPER);
    }

    public JobInfo findById(Long id) {
        List<JobInfo> r = jdbc.query("SELECT * FROM job_info WHERE id = ?", MAPPER, id);
        return r.isEmpty() ? null : r.get(0);
    }

    public JobInfo findByName(String name) {
        List<JobInfo> r = jdbc.query("SELECT * FROM job_info WHERE job_name = ?", MAPPER, name);
        return r.isEmpty() ? null : r.get(0);
    }

    public Long insert(JobInfo j) {
        KeyHolder kh = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement("""
                    INSERT INTO job_info
                      (job_name, cron_expr, handler_type, handler_value, param,
                       timeout_sec, max_retry, misfire_strategy, status, next_trigger_time)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, j.getJobName());
            ps.setString(2, j.getCronExpr());
            ps.setInt(3, j.getHandlerType());
            ps.setString(4, j.getHandlerValue());
            ps.setString(5, j.getParam());
            ps.setInt(6, j.getTimeoutSec());
            ps.setInt(7, j.getMaxRetry());
            ps.setInt(8, j.getMisfireStrategy());
            ps.setInt(9, j.getStatus());
            ps.setObject(10, j.getNextTriggerTime());
            return ps;
        }, kh);
        return kh.getKey().longValue();
    }

    public int update(JobInfo j) {
        return jdbc.update("""
                UPDATE job_info
                   SET cron_expr = ?, handler_type = ?, handler_value = ?, param = ?,
                       timeout_sec = ?, max_retry = ?, misfire_strategy = ?,
                       status = ?, next_trigger_time = ?
                 WHERE id = ?
                """,
                j.getCronExpr(), j.getHandlerType(), j.getHandlerValue(), j.getParam(),
                j.getTimeoutSec(), j.getMaxRetry(), j.getMisfireStrategy(),
                j.getStatus(),
                j.getNextTriggerTime(),
                j.getId());
    }

    public int setStatus(Long id, int status) {
        return jdbc.update("UPDATE job_info SET status = ? WHERE id = ?", status, id);
    }

    public int delete(Long id) {
        return jdbc.update("DELETE FROM job_info WHERE id = ?", id);
    }
}
