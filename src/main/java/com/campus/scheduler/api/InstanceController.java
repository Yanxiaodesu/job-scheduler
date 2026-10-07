package com.campus.scheduler.api;

import com.campus.scheduler.model.JobInstance;
import com.campus.scheduler.repo.JobInstanceRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 实例查询。
 *
 * <p>单独一个 Controller 而不是塞进 {@code JobController}：
 * 因为 {@code JobController} 的类级路径是 {@code /api/jobs}，
 * 加一个 {@code /api/jobs/instances} 会和 {@code /api/jobs/{id}} 撞路由。
 *
 * <p>这个接口主要给两类使用者：压测脚本（要统计重复数、精度），
 * 以及排查问题时要看「最近都跑了什么」。
 */
@RestController
@RequestMapping("/api/instances")
@RequiredArgsConstructor
public class InstanceController {

    private final JobInstanceRepo instanceRepo;
    private final JdbcTemplate jdbc;

    /** 最近的实例列表，带任务名 */
    @GetMapping
    public List<Map<String, Object>> recent(@RequestParam(defaultValue = "50") int limit) {
        return jdbc.queryForList("""
                SELECT i.id, j.job_name, i.trigger_time, i.start_time, i.end_time,
                       i.status, i.retry_count, e.address AS executor,
                       i.result_msg
                  FROM job_instance i
                  JOIN job_info j ON j.id = i.job_id
                  LEFT JOIN executor_registry e ON e.id = i.executor_id
                 ORDER BY i.id DESC
                 LIMIT ?
                """, Math.min(limit, 1000));
    }

    /**
     * 校验「恰好一次」：同一任务同一触发时刻有没有出现多条实例。
     *
     * <p>这是验证「零重复」的官方口径 —— 压测脚本直接调它，
     * 而不是自己写 SQL，免得口径不一致。
     */
    @GetMapping("/duplicates")
    public Map<String, Object> duplicates() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("total", jdbc.queryForObject("SELECT COUNT(*) FROM job_instance", Integer.class));
        m.put("distinctTriggerTimes",
                jdbc.queryForObject("SELECT COUNT(DISTINCT instance_key) FROM job_instance", Integer.class));
        Integer dup = jdbc.queryForObject("""
                SELECT COUNT(*) FROM (
                  SELECT instance_key FROM job_instance
                   GROUP BY instance_key HAVING COUNT(*) > 1
                ) t
                """, Integer.class);
        m.put("duplicateKeys", dup == null ? 0 : dup);
        m.put("ok", dup == null || dup == 0);
        return m;
    }

    /**
     * 调度精度：实际派发时刻 − 计划触发时刻（毫秒）。
     *
     * <p>直接算好平均/P50/P95/最大返回，调用方不用自己写 SQL。
     */
    @GetMapping("/precision")
    public Map<String, Object> precision() {
        Map<String, Object> m = new LinkedHashMap<>();
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM job_instance WHERE start_time IS NOT NULL", Integer.class);
        m.put("samples", n);
        if (n == null || n == 0) {
            return m;
        }
        m.put("avgMs", round(jdbc.queryForObject("""
                SELECT AVG(TIMESTAMPDIFF(MICROSECOND, trigger_time, start_time)/1000)
                  FROM job_instance WHERE start_time IS NOT NULL
                """, Double.class)));
        m.put("minMs", round(jdbc.queryForObject("""
                SELECT MIN(TIMESTAMPDIFF(MICROSECOND, trigger_time, start_time)/1000)
                  FROM job_instance WHERE start_time IS NOT NULL
                """, Double.class)));
        m.put("maxMs", round(jdbc.queryForObject("""
                SELECT MAX(TIMESTAMPDIFF(MICROSECOND, trigger_time, start_time)/1000)
                  FROM job_instance WHERE start_time IS NOT NULL
                """, Double.class)));
        m.put("p50Ms", percentile(0.50));
        m.put("p95Ms", percentile(0.95));
        m.put("p99Ms", percentile(0.99));
        return m;
    }

    /**
     * 手算分位数。MySQL 8 没有 {@code PERCENTILE_CONT}，用窗口函数自己取。
     */
    private Double percentile(double p) {
        return round(jdbc.queryForObject("""
                SELECT d FROM (
                  SELECT TIMESTAMPDIFF(MICROSECOND, trigger_time, start_time)/1000 AS d,
                         ROW_NUMBER() OVER (ORDER BY TIMESTAMPDIFF(MICROSECOND, trigger_time, start_time)) AS rn,
                         COUNT(*) OVER () AS n
                    FROM job_instance WHERE start_time IS NOT NULL
                ) t
                 WHERE rn = GREATEST(1, FLOOR(n * ?))
                 LIMIT 1
                """, Double.class, p));
    }

    private Double round(Double v) {
        return v == null ? null : Math.round(v * 10) / 10.0;
    }

    /** 清空所有实例和日志（压测前重置用） */
    @org.springframework.web.bind.annotation.DeleteMapping("/reset")
    public Map<String, Object> reset() {
        jdbc.update("DELETE FROM job_log");
        int n = jdbc.update("DELETE FROM job_instance");
        return Map.of("ok", true, "deleted", n);
    }

    public int countByStatus(int status) {
        return instanceRepo.countByStatus(status);
    }

    public static final int RUNNING = JobInstance.RUNNING;
}
