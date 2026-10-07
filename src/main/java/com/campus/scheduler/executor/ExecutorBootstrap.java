package com.campus.scheduler.executor;

import com.campus.scheduler.config.SchedulerProperties;
import com.campus.scheduler.repo.ExecutorRepo;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 执行器上线：注册自己 + 定期心跳。
 *
 * <p>心跳表就是 Admin 判断「谁还活着」的唯一依据，所以这个类虽然只有几行，
 * 却是故障转移能工作的前提 —— 没有心跳，Admin 根本不知道执行器挂了。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExecutorBootstrap {

    private final SchedulerProperties props;
    private final ExecutorRepo executorRepo;

    /** 启动时立刻注册一次，别等 10 秒后的第一次心跳 */
    @PostConstruct
    public void registerOnStart() {
        if (!props.isExecutor()) {
            return;
        }
        try {
            executorRepo.heartbeat(props.getExecutor().getAppName(), props.getExecutor().getAddress());
            log.info("执行器已注册 app={} address={}",
                    props.getExecutor().getAppName(), props.getExecutor().getAddress());
        } catch (Exception e) {
            log.error("执行器注册失败（数据库没起来？稍后靠心跳补上）", e);
        }
    }

    /**
     * 定期心跳。用 UPSERT 一条 SQL 完成「首次注册 / 后续续约」两种情况。
     *
     * <p>心跳间隔（10s）必须明显小于离线判定阈值（30s），否则网络抖一下就误判离线。
     * 一般是 1:3 的比例。
     */
    @Scheduled(fixedDelayString = "${scheduler.executor.heartbeat-interval-ms:10000}")
    public void heartbeat() {
        if (!props.isExecutor()) {
            return;
        }
        try {
            executorRepo.heartbeat(props.getExecutor().getAppName(), props.getExecutor().getAddress());
        } catch (Exception e) {
            log.warn("心跳失败：{}", e.getMessage());
        }
    }
}
