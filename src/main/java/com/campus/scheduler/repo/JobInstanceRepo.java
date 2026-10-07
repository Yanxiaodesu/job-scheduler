package com.campus.scheduler.repo;

import com.campus.scheduler.model.JobInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class JobInstanceRepo {

    private final JdbcTemplate jdbc;

    public static final RowMapper<JobInstance> MAPPER = (rs, i) -> {
        JobInstance t = new JobInstance();
        t.setId(rs.getLong("id"));
        t.setJobId(rs.getLong("job_id"));
        long eid = rs.getLong("executor_id");
        t.setExecutorId(rs.wasNull() ? null : eid);
        Timestamp tt = rs.getTimestamp("trigger_time");
        t.setTriggerTime(tt == null ? null : tt.toLocalDateTime());
        t.setInstanceKey(rs.getString("instance_key"));
        Timestamp st = rs.getTimestamp("start_time");
        t.setStartTime(st == null ? null : st.toLocalDateTime());
        Timestamp et = rs.getTimestamp("end_time");
        t.setEndTime(et == null ? null : et.toLocalDateTime());
        t.setStatus(rs.getInt("status"));
        t.setRetryCount(rs.getInt("retry_count"));
        t.setResultMsg(rs.getString("result_msg"));
        Timestamp ct = rs.getTimestamp("create_time");
        t.setCreateTime(ct == null ? null : ct.toLocalDateTime());
        return t;
    };

    /**
     * ★★ 「恰好一次」的第二层保险。
     *
     * <p>表上有 {@code UNIQUE KEY uk_instance (instance_key)}，
     * instance_key = "jobId:触发时刻"。所以：
     *
     * <ul>
     *   <li>插入成功 → 这次触发归我，返回实例 id</li>
     *   <li>抛 DuplicateKeyException → 别人已经为这个触发时刻落过实例了，
     *       返回 null 让调用方跳过</li>
     * </ul>
     *
     * <p><b>为什么这层必须有</b>：第一层的乐观锁只能保证「推进 next_trigger_time」
     * 这件事只有一个人做成，但它保证不了「不会落两条实例」——
     * 比如 Admin 推进时间后崩溃重启，重新扫到时 next_trigger_time 已经变了，
     * 但崩溃前那条实例可能只落了一半。唯一键是数据库层面的最终保证。
     *
     * @return 实例 id；返回 null 表示这次触发已被别人抢先
     */
    public Long tryCreate(Long jobId, java.time.LocalDateTime triggerTime, String instanceKey) {
        KeyHolder kh = new GeneratedKeyHolder();
        try {
            jdbc.update(con -> {
                PreparedStatement ps = con.prepareStatement("""
                        INSERT INTO job_instance (job_id, trigger_time, instance_key, status)
                        VALUES (?,?,?,?)
                        """, Statement.RETURN_GENERATED_KEYS);
                ps.setLong(1, jobId);
                ps.setTimestamp(2, Timestamp.valueOf(triggerTime));
                ps.setString(3, instanceKey);
                ps.setInt(4, JobInstance.PENDING);
                return ps;
            }, kh);
            return kh.getKey().longValue();
        } catch (DuplicateKeyException e) {
            // 别人已经为这个触发时刻建过实例了 —— 这是正常的并发结果，不是错误
            return null;
        }
    }

    /**
     * 记录「已派发给哪个执行器」，同时把状态推进到执行中。
     *
     * <p>把这两件事合成一句 SQL 是有意为之：从调度中心的角度，
     * 「派发出去了」就等于「开始跑了」。这样状态语义很清晰：
     * <ul>
     *   <li>{@code PENDING} = 还没派给任何人（含失败重试退回来的）</li>
     *   <li>{@code RUNNING} = 已经派出去了，等执行器回调</li>
     * </ul>
     * 语义清晰之后，{@code findPendingUnassigned}（找没派出去的）和
     * {@code findStuckOnOfflineExecutor}（找派出去了但执行器挂了的）
     * 两个查询才能各管各的、不重叠。
     */
    public int markDispatched(Long id, Long executorId) {
        return jdbc.update("""
                UPDATE job_instance
                   SET executor_id = ?, status = ?, start_time = NOW(3)
                 WHERE id = ?
                """, executorId, JobInstance.RUNNING, id);
    }

    public int markFinished(Long id, int status, String msg) {
        return jdbc.update("""
                UPDATE job_instance
                   SET status = ?, end_time = NOW(3), result_msg = ?
                 WHERE id = ?
                """, status, msg == null ? null : msg.substring(0, Math.min(msg.length(), 900)), id);
    }

    /**
     * ★ 派发失败后退回「待执行」。
     *
     * <p><b>为什么必须有这个方法</b>：{@link #markDispatched} 会先把实例标成
     * 「执行中 + 已分配执行器」，然后才发 HTTP。如果 HTTP 失败了，实例就卡在
     * 一个很尴尬的状态：
     *
     * <ul>
     *   <li>{@code findPendingUnassigned}（重派）要求 {@code executor_id IS NULL} —— 捞不到</li>
     *   <li>{@code findStuckOnOfflineExecutor}（离线转移）要求执行器离线 ——
     *       可执行器是好的，只是这次没送达，也捞不到</li>
     *   <li>结果只有超时看门狗能在几十秒后兜住它，而且会被判成「超时」</li>
     * </ul>
     *
     * <p>退回 PENDING 并清空 executor_id 之后，重派循环 5 秒内就能重新发出去。
     *
     * <p>注意<b>不增加</b> retry_count：这次压根没送到执行器手上，
     * 不算一次真正的执行尝试。
     */
    public int revertToPending(Long id, String msg) {
        return jdbc.update("""
                UPDATE job_instance
                   SET status = ?, executor_id = NULL, start_time = NULL, result_msg = ?
                 WHERE id = ?
                """, JobInstance.PENDING,
                msg == null ? null : msg.substring(0, Math.min(msg.length(), 900)), id);
    }

    /**
     * 失败重试：退回 PENDING，重试次数 +1。
     *
     * <p>和 {@link #revertToPending} 的区别：这个是「执行过了但失败」，
     * 要计入重试次数；那个是「根本没送达」，不计。
     */
    public int markRetry(Long id, String msg) {
        return jdbc.update("""
                UPDATE job_instance
                   SET status = ?, executor_id = NULL, start_time = NULL,
                       retry_count = retry_count + 1, result_msg = ?
                 WHERE id = ?
                """, JobInstance.PENDING,
                msg == null ? null : msg.substring(0, Math.min(msg.length(), 900)), id);
    }

    /**
     * ★ 故障转移用：找出「派给了已离线的执行器、但还没跑完」的实例。
     *
     * <p>这就是「执行器宕机后任务不丢」的实现。
     * 只挑 PENDING / RUNNING 两种状态 —— 已经成功或失败的不该重跑。
     */
    public List<JobInstance> findStuckOnOfflineExecutor(long offlineThresholdSec) {
        return jdbc.query("""
                SELECT i.* FROM job_instance i
                 JOIN executor_registry e ON e.id = i.executor_id
                 WHERE i.status IN (?, ?)
                   AND (e.status = 0 OR e.last_heartbeat <= NOW() - INTERVAL ? SECOND)
                 ORDER BY i.id
                 LIMIT 200
                """, MAPPER, JobInstance.PENDING, JobInstance.RUNNING, offlineThresholdSec);
    }

    /**
     * ★ misfire 用：找出「触发时刻已经过去很久、但一直没人执行」的实例。
     *
     * <p>典型场景是调度中心停机了一段时间，重启后这些实例还挂在那里。
     */
    public List<JobInstance> findMisfired(long thresholdSec) {
        return jdbc.query("""
                SELECT * FROM job_instance
                 WHERE status = ?
                   AND trigger_time <= NOW() - INTERVAL ? SECOND
                 ORDER BY trigger_time
                 LIMIT 200
                """, MAPPER, JobInstance.PENDING, thresholdSec);
    }

    /**
     * ★ 重新派发用：找出「待执行但还没分配给任何执行器」的实例。
     *
     * <p>会有两类实例落在这里：
     * <ol>
     *   <li><b>失败重试</b>：{@link #markRetry} 把它退回 PENDING 并清空了 executor_id</li>
     *   <li><b>派发时没有可用执行器</b>：实例建好了但一直没人接</li>
     * </ol>
     *
     * <p>为什么要 {@code create_time <= NOW() - INTERVAL ? SECOND} 这个时间窗：
     * 正常路径是「建实例 → 立刻 markDispatched」，中间有几十毫秒的空窗期。
     * 如果不加时间窗，重派任务可能正好插进这个空窗，导致同一个实例被派发两次。
     * 加了几秒的宽限就不会误伤正常路径。执行器侧还有幂等兜底，双保险。
     */
    public List<JobInstance> findPendingUnassigned(int graceSec) {
        return jdbc.query("""
                SELECT * FROM job_instance
                 WHERE status = ?
                   AND executor_id IS NULL
                   AND create_time <= NOW() - INTERVAL ? SECOND
                 ORDER BY id
                 LIMIT 200
                """, MAPPER, JobInstance.PENDING, graceSec);
    }

    /**
     * ★ 执行超时看门狗：找出「执行中但早就该结束了」的实例。
     *
     * <p>为什么必须有这一步：回调是走网络的，<b>会丢</b>。执行器可能
     * 执行到一半进程被杀、或者回调时网络断了、或者中心重启错过了请求。
     * 没有看门狗，这些实例会永远卡在 RUNNING，任务等于石沉大海。
     *
     * <p>超时阈值取自任务自己的 {@code timeout_sec}，再加一点宽限（grace），
     * 避免网络稍慢就把正常任务误判成超时。
     *
     * <p>它和「执行器离线转移」是两回事，覆盖的场景不同：
     * <ul>
     *   <li>执行器离线转移 → 那个执行器<b>整个</b>没了，靠心跳发现</li>
     *   <li>超时看门狗 → 执行器还活着，但某个任务<b>卡住或回调丢了</b></li>
     * </ul>
     */
    public List<JobInstance> findTimeout(int graceSec) {
        return jdbc.query("""
                SELECT i.* FROM job_instance i
                 JOIN job_info j ON j.id = i.job_id
                 WHERE i.status = ?
                   AND i.start_time IS NOT NULL
                   AND i.start_time <= NOW() - INTERVAL (j.timeout_sec + ?) SECOND
                 ORDER BY i.start_time
                 LIMIT 200
                """, MAPPER, JobInstance.RUNNING, graceSec);
    }

    public List<JobInstance> findByJob(Long jobId, int limit) {
        return jdbc.query(
                "SELECT * FROM job_instance WHERE job_id = ? ORDER BY id DESC LIMIT ?",
                MAPPER, jobId, limit);
    }

    public JobInstance findById(Long id) {
        List<JobInstance> r = jdbc.query("SELECT * FROM job_instance WHERE id = ?", MAPPER, id);
        return r.isEmpty() ? null : r.get(0);
    }

    public int countByStatus(int status) {
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM job_instance WHERE status = ?", Integer.class, status);
        return n == null ? 0 : n;
    }
}
