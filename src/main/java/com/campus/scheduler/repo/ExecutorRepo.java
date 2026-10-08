package com.campus.scheduler.repo;

import com.campus.scheduler.model.ExecutorNode;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class ExecutorRepo {

    private final JdbcTemplate jdbc;

    public static final RowMapper<ExecutorNode> MAPPER = (rs, i) -> {
        ExecutorNode e = new ExecutorNode();
        e.setId(rs.getLong("id"));
        e.setAppName(rs.getString("app_name"));
        e.setAddress(rs.getString("address"));
        // 时间列用 LocalDateTime 读，不要用 Timestamp —— 见 JobInfoRepo.MAPPER 的长注释
        e.setLastHeartbeat(rs.getObject("last_heartbeat", LocalDateTime.class));
        e.setStatus(rs.getInt("status"));
        e.setCreateTime(rs.getObject("create_time", LocalDateTime.class));
        return e;
    };

    /**
     * 执行器注册 / 续约。用 UPSERT 一条语句搞定：
     * 第一次是 INSERT，之后每次心跳都是 UPDATE，不用先查再判断。
     */
    public void heartbeat(String appName, String address) {
        jdbc.update("""
                INSERT INTO executor_registry (app_name, address, last_heartbeat, status)
                VALUES (?, ?, NOW(), 1)
                ON DUPLICATE KEY UPDATE last_heartbeat = NOW(), status = 1
                """, appName, address);
    }

    /**
     * ★ 取健康的执行器 —— 路由的候选集。
     *
     * <p>`last_heartbeat > NOW() - INTERVAL ? SECOND` 就是心跳超时判定。
     * 注意这里用数据库的 NOW() 而不是应用机器的时钟：
     * 多台机器时钟不同步时，用统一的数据库时间更可靠。
     */
    public List<ExecutorNode> findHealthy(String appName, long offlineThresholdSec) {
        return jdbc.query("""
                SELECT * FROM executor_registry
                 WHERE app_name = ?
                   AND status = 1
                   AND last_heartbeat > NOW() - INTERVAL ? SECOND
                 ORDER BY id
                """, MAPPER, appName, offlineThresholdSec);
    }

    /**
     * ★ 把心跳超时的执行器标记为离线。
     *
     * @return 被标记的数量。>0 说明有执行器挂了，故障转移该干活了
     */
    public int markOffline(String appName, long offlineThresholdSec) {
        return jdbc.update("""
                UPDATE executor_registry
                   SET status = 0
                 WHERE app_name = ?
                   AND status = 1
                   AND last_heartbeat <= NOW() - INTERVAL ? SECOND
                """, appName, offlineThresholdSec);
    }

    public List<ExecutorNode> findAll() {
        return jdbc.query("SELECT * FROM executor_registry ORDER BY app_name, id", MAPPER);
    }

    public ExecutorNode findByAddress(String appName, String address) {
        List<ExecutorNode> r = jdbc.query(
                "SELECT * FROM executor_registry WHERE app_name = ? AND address = ?",
                MAPPER, appName, address);
        return r.isEmpty() ? null : r.get(0);
    }
}
