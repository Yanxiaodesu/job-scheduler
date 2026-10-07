package com.campus.scheduler.executor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 内置 handler 注册表 + 四个示例任务。
 *
 * <p>这四个示例不是凑数，它们分别覆盖一类需要在压测里验证的场景：
 *
 * <ul>
 *   <li>{@code printTime} —— 正常任务，用来压「调度精度」</li>
 *   <li>{@code sleep}     —— 慢任务，用来压「执行超时」和「执行器宕机转移」</li>
 *   <li>{@code alwaysFail}—— 必失败，用来验证「失败重试」和「重试次数上限」</li>
 *   <li>{@code busy}      —— CPU 忙等，用来压「并发执行」</li>
 * </ul>
 */
@Slf4j
@Component
public class DemoHandlers {

    private final Map<String, JobHandler> registry;

    public DemoHandlers(List<JobHandler> handlers) {
        // 把所有 JobHandler Bean 按 name() 收进 Map，派发时按名字取
        this.registry = handlers.stream()
                .collect(Collectors.toMap(JobHandler::name, Function.identity()));
        log.info("已注册内置 handler: {}", registry.keySet());
    }

    public JobHandler get(String name) {
        return registry.get(name);
    }

    public boolean has(String name) {
        return registry.containsKey(name);
    }

    // ==================== 四个示例 handler ====================

    /** 打印当前时间。正常任务，跑得很快。 */
    @Component
    @Slf4j
    public static class PrintTime implements JobHandler {
        @Override
        public String name() {
            return "printTime";
        }

        @Override
        public String execute(Long instanceId, String param) {
            String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
            log.info("[printTime] instance={} now={}", instanceId, now);
            return "执行时间 " + now;
        }
    }

    /**
     * 睡 N 秒（param 里给秒数，默认 3）。
     * 用来验证执行超时、以及「任务还在跑时执行器被 kill」的故障转移。
     */
    @Component
    @Slf4j
    public static class Sleep implements JobHandler {
        @Override
        public String name() {
            return "sleep";
        }

        @Override
        public String execute(Long instanceId, String param) throws InterruptedException {
            int sec = 3;
            try {
                if (param != null && !param.isBlank()) {
                    sec = Integer.parseInt(param.trim());
                }
            } catch (NumberFormatException ignored) {
                // 参数写错就按默认值走，不让任务因为这个失败
            }
            log.info("[sleep] instance={} 开始睡 {}s", instanceId, sec);
            Thread.sleep(sec * 1000L);
            log.info("[sleep] instance={} 睡醒了", instanceId);
            return "睡了 " + sec + " 秒";
        }
    }

    /** 必定失败。用来验证失败重试和 retry_count 上限。 */
    @Component
    public static class AlwaysFail implements JobHandler {
        @Override
        public String name() {
            return "alwaysFail";
        }

        @Override
        public String execute(Long instanceId, String param) {
            throw new IllegalStateException("这是故意抛的异常，用来验证失败重试（instance=" + instanceId + "）");
        }
    }

    /** CPU 忙等 N 毫秒，用来压并发。 */
    @Component
    public static class Busy implements JobHandler {
        @Override
        public String name() {
            return "busy";
        }

        @Override
        public String execute(Long instanceId, String param) {
            long ms = 200;
            try {
                if (param != null && !param.isBlank()) {
                    ms = Long.parseLong(param.trim());
                }
            } catch (NumberFormatException ignored) {
                // 同上
            }
            long end = System.currentTimeMillis() + ms;
            double x = 0;
            while (System.currentTimeMillis() < end) {
                x += Math.sqrt(x + 1);
            }
            return "忙等 " + ms + "ms";
        }
    }
}
