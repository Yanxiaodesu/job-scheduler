package com.campus.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 分布式定时任务调度平台。
 *
 * <p>{@code @EnableScheduling} 是这个应用的心脏开关 —— 它让 Spring 去扫描
 * 所有 {@code @Scheduled} 方法，我们的三个核心循环都挂在这个机制上：
 *
 * <ul>
 *   <li>{@code SchedulerLoop#scan} —— 每 1 秒扫到点任务（调度）</li>
 *   <li>{@code FailoverTask#run} —— 每 30 秒做故障转移和 misfire</li>
 *   <li>{@code FailoverTask#retryPending} —— 每 5 秒重派未派发的实例</li>
 * </ul>
 *
 * <p>启动方式（同一个 jar，靠环境变量切换角色）：
 * <pre>
 * # 调度中心
 * ROLE=admin PORT=8080 java -jar job-scheduler.jar
 *
 * # 两个执行器
 * ROLE=executor PORT=9001 ADDRESS=http://127.0.0.1:9001 java -jar job-scheduler.jar
 * ROLE=executor PORT=9002 ADDRESS=http://127.0.0.1:9002 java -jar job-scheduler.jar
 * </pre>
 */
@SpringBootApplication
@EnableScheduling
public class JobSchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(JobSchedulerApplication.class, args);
    }
}
