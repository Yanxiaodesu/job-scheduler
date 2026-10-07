package com.campus.scheduler.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 调度相关配置。
 *
 * <p>为什么把 role 做成配置项：一个代码库要能跑三种角色（admin / executor / both），
 * 这样压测「多 Admin 并发不重复派发」时，只要起三个不同端口的实例就行了，
 * 不需要维护两套代码。
 */
@Data
@Component
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    /** admin=只调度，executor=只执行，both=单进程双角色 */
    private String role = "both";

    /** 本实例标识，抢锁和日志里用来区分 */
    private String instanceId = "local";

    /** 调度循环扫描间隔（毫秒） */
    private long scanIntervalMs = 1000;

    /** 故障转移扫描间隔（毫秒） */
    private long failoverIntervalMs = 30000;

    /** 超过这个秒数还没执行的任务算 misfire */
    private long misfireThresholdSec = 300;

    /**
     * Admin 自己的回调地址，会随派发请求一起发给执行器。
     *
     * <p>为什么让 Admin 把地址「带过去」，而不是让执行器配置 Admin 的地址：
     * 这样执行器不需要知道 Admin 部署在哪，换 Admin 也不用改执行器配置。
     * 执行完直接回调这个 URL 就行。
     */
    private String callbackUrl = "http://127.0.0.1:8080/api/internal/callback";

    private Executor executor = new Executor();

    @Data
    public static class Executor {
        /** 执行器分组名 */
        private String appName = "demo";
        /** 本执行器回调地址 */
        private String address = "http://127.0.0.1:8080";
        /** 心跳间隔（毫秒） */
        private long heartbeatIntervalMs = 10000;
        /** 判定离线的心跳超时（秒） */
        private long offlineThresholdSec = 30;
    }

    public boolean isAdmin() {
        return "admin".equalsIgnoreCase(role) || "both".equalsIgnoreCase(role);
    }

    public boolean isExecutor() {
        return "executor".equalsIgnoreCase(role) || "both".equalsIgnoreCase(role);
    }
}
