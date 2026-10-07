package com.campus.scheduler.repo;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/** 执行日志。出问题时靠它回答「这个任务到底为什么失败」。 */
@Repository
@RequiredArgsConstructor
public class JobLogRepo {

    private final JdbcTemplate jdbc;

    public void info(Long instanceId, String content) {
        append(instanceId, "INFO", content);
    }

    public void error(Long instanceId, String content) {
        append(instanceId, "ERROR", content);
    }

    public void append(Long instanceId, String level, String content) {
        if (content == null) {
            content = "";
        }
        jdbc.update("INSERT INTO job_log (instance_id, level, content) VALUES (?,?,?)",
                instanceId, level, content.substring(0, Math.min(content.length(), 950)));
    }

    public List<Map<String, Object>> findByInstance(Long instanceId) {
        return jdbc.queryForList(
                "SELECT log_time, level, content FROM job_log WHERE instance_id = ? ORDER BY id",
                instanceId);
    }
}
