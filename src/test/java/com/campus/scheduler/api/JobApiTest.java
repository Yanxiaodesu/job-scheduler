package com.campus.scheduler.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * HTTP 接口层测试。
 *
 * <h2>为什么单独测这一层</h2>
 *
 * <p>业务逻辑的测试再多，也挡不住「路由写错、参数没校验、该 404 的返回了 200」
 * 这类问题 —— 它们只在 HTTP 边界上暴露。而这个项目所有功能都是通过 HTTP 暴露的
 * （没有前端），接口层就是产品本身。
 *
 * <p>用 MockMvc 而不是真起服务：不占端口、跑得快、能精确断言状态码和 body。
 * 真实端到端由 CI 里的 e2e job 负责。
 *
 * <p><b>注意 Spring Boot 4 的两处变化</b>（踩过一次）：
 * <ul>
 *   <li>{@code @AutoConfigureMockMvc} 挪到了
 *       {@code org.springframework.boot.webmvc.test.autoconfigure}</li>
 *   <li>Jackson 升到 3.x，包名从 {@code com.fasterxml.jackson} 变成
 *       {@code tools.jackson}。这里干脆不用它的 API，手写 JSON，
 *       免得再被版本迁移坑一次</li>
 * </ul>
 */
@SpringBootTest(properties = {
        "scheduler.role=admin",
        // 关掉所有后台循环，避免它们干扰断言
        "scheduler.scan-interval-ms=3600000",
        "scheduler.failover-interval-ms=3600000",
        "scheduler.retry-interval-ms=3600000",
        "scheduler.prefetch-interval-ms=3600000",
        "scheduler.prefetch-enabled=false"
})
@AutoConfigureMockMvc
class JobApiTest {

    @Autowired
    private MockMvc mvc;
    @Autowired
    private JdbcTemplate jdbc;

    private static final Pattern ID = Pattern.compile("\"id\"\\s*:\\s*(\\d+)");

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM job_instance");
        jdbc.update("DELETE FROM job_log");
        jdbc.update("DELETE FROM executor_registry");
        jdbc.update("DELETE FROM job_info");
    }

    /** 手工拼一个建任务的 JSON，避免依赖 Jackson 的 API */
    private String body(Map<String, Object> overrides) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("jobName", "api-test-" + System.nanoTime());
        m.put("cronExpr", "*/5 * * * * *");
        m.put("handlerType", 1);
        m.put("handlerValue", "printTime");
        m.put("timeoutSec", 30);
        m.put("maxRetry", 0);
        m.put("misfireStrategy", 2);
        m.put("status", 1);
        m.putAll(overrides);

        StringBuilder sb = new StringBuilder("{");
        m.forEach((k, v) -> {
            sb.append('"').append(k).append("\":");
            if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
            } else if (v == null) {
                sb.append("null");
            } else {
                sb.append('"').append(v).append('"');
            }
            sb.append(',');
        });
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.setLength(sb.length() - 1);
        }
        return sb.append('}').toString();
    }

    private long createJob(Map<String, Object> overrides) throws Exception {
        String resp = mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(overrides)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        Matcher matcher = ID.matcher(resp);
        assertTrue(matcher.find(), "建任务的响应里应该有 id：" + resp);
        return Long.parseLong(matcher.group(1));
    }

    // ==================== 建任务 ====================

    @Test
    @DisplayName("建任务：成功后返回 id 和算好的下次触发时间")
    void createReturnsIdAndNextTriggerTime() throws Exception {
        mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(Map.of())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").isNumber())
                // ★ 必须把 next_trigger_time 算好，否则任务永远不会被扫到
                .andExpect(jsonPath("$.nextTriggerTime").isNotEmpty());
    }

    @Test
    @DisplayName("建任务：非法 cron 返回 400，且带可读的错误信息")
    void invalidCronRejected() throws Exception {
        mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(Map.of("cronExpr", "not-a-cron"))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").isNotEmpty());
    }

    @Test
    @DisplayName("建任务：5 段式 cron 也要拒绝（本项目是 6 段式）")
    void fiveFieldCronRejected() throws Exception {
        // 5 段式是 Linux crontab 的写法，很容易写错。
        // 6 段式第一个字段是秒 —— 写错了任务会按完全错误的时间跑
        mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(Map.of("cronExpr", "*/5 * * * *"))))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("建任务：重名返回 400")
    void duplicateNameRejected() throws Exception {
        createJob(Map.of("jobName", "same-name"));
        mvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(Map.of("jobName", "same-name"))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").isNotEmpty());
    }

    // ==================== 查询 ====================

    @Test
    @DisplayName("列表：返回所有任务")
    void listReturnsJobs() throws Exception {
        createJob(Map.of("jobName", "list-a"));
        createJob(Map.of("jobName", "list-b"));
        mvc.perform(get("/api/jobs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    @DisplayName("详情：不存在的 id 返回 404，而不是 200 + 空 body")
    void getMissingReturns404() throws Exception {
        mvc.perform(get("/api/jobs/999999"))
                .andExpect(status().isNotFound());
    }

    // ==================== 启停 ====================

    @Test
    @DisplayName("停用：status 变成 0")
    void disable() throws Exception {
        long id = createJob(Map.of());
        mvc.perform(post("/api/jobs/" + id + "/disable"))
                .andExpect(status().isOk());
        assertEquals(0, (int) jdbc.queryForObject(
                "SELECT status FROM job_info WHERE id = ?", Integer.class, id));
    }

    @Test
    @DisplayName("启用：重算下次触发时间（否则一启用就被判成 misfire）")
    void enableRecalculatesTriggerTime() throws Exception {
        long id = createJob(Map.of());
        mvc.perform(post("/api/jobs/" + id + "/disable")).andExpect(status().isOk());

        // 把触发时间改到很久以前，模拟「停用了很久」
        jdbc.update("UPDATE job_info SET next_trigger_time = NOW(3) - INTERVAL 10 DAY WHERE id = ?", id);

        mvc.perform(post("/api/jobs/" + id + "/enable"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nextTriggerTime").isNotEmpty());

        var t = jdbc.queryForObject(
                "SELECT next_trigger_time FROM job_info WHERE id = ?", java.sql.Timestamp.class, id);
        assertTrue(t.toLocalDateTime().isAfter(LocalDateTime.now().minusMinutes(1)),
                "启用后应该重算成未来的时间点，不能保留停用期间那个已经过去的时间");
    }

    @Test
    @DisplayName("删除：任务从库里消失")
    void deleteRemovesJob() throws Exception {
        long id = createJob(Map.of());
        mvc.perform(delete("/api/jobs/" + id)).andExpect(status().isOk());
        assertEquals(0, (int) jdbc.queryForObject(
                "SELECT COUNT(*) FROM job_info WHERE id = ?", Integer.class, id));
    }

    // ==================== 手动触发 ====================

    @Test
    @DisplayName("手动触发：落一条实例，且不影响 cron 排期")
    void manualTriggerDoesNotDisturbSchedule() throws Exception {
        long id = createJob(Map.of());
        var before = jdbc.queryForObject(
                "SELECT next_trigger_time FROM job_info WHERE id = ?", java.sql.Timestamp.class, id);

        mvc.perform(post("/api/jobs/" + id + "/trigger"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.instanceId").isNumber());

        assertEquals(1, (int) jdbc.queryForObject(
                "SELECT COUNT(*) FROM job_instance", Integer.class));

        var after = jdbc.queryForObject(
                "SELECT next_trigger_time FROM job_info WHERE id = ?", java.sql.Timestamp.class, id);
        assertEquals(before.toLocalDateTime(), after.toLocalDateTime(),
                "手动触发不该打乱正常排期");
    }

    @Test
    @DisplayName("手动触发：不存在的任务返回 404")
    void manualTriggerMissingJob() throws Exception {
        mvc.perform(post("/api/jobs/999999/trigger"))
                .andExpect(status().isNotFound());
    }

    // ==================== 指标与校验口径 ====================

    @Test
    @DisplayName("指标接口：返回调度/故障转移/实例/预取统计")
    void metricsShape() throws Exception {
        mvc.perform(get("/api/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.role").value("admin"))
                .andExpect(jsonPath("$.scheduler.triggered").exists())
                // lostTheRace 是「恰好一次」的验证口径，必须暴露出来
                .andExpect(jsonPath("$.scheduler.lostTheRace").exists())
                .andExpect(jsonPath("$.failover.transferred").exists())
                .andExpect(jsonPath("$.instances.success").exists())
                .andExpect(jsonPath("$.prefetch.enabled").exists());
    }

    @Test
    @DisplayName("重复检查接口：口径固定，初始状态就是 ok")
    void duplicatesEndpoint() throws Exception {
        mvc.perform(get("/api/instances/duplicates"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicateKeys").value(0))
                .andExpect(jsonPath("$.ok").value(true));
    }

    @Test
    @DisplayName("精度接口：没有样本时不报错，样本数返回 0")
    void precisionWithoutSamples() throws Exception {
        mvc.perform(get("/api/instances/precision"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.samples").value(0));
    }

    @Test
    @DisplayName("实例列表：空库返回空数组而不是 500")
    void recentInstancesEmpty() throws Exception {
        mvc.perform(get("/api/instances").param("limit", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }
}
