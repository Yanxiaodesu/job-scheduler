package com.campus.scheduler.core;

import com.campus.scheduler.model.ExecutorNode;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 路由：从健康的执行器里挑一个。
 *
 * <p>xxl-job 支持十来种路由策略（第一个、最后一个、轮询、随机、一致性哈希、
 * 最不经常使用、故障转移、分片广播……），我们只保留两种 —— 因为
 * <b>路由策略本身不是这个项目的考点</b>，「为什么需要路由」才是。
 *
 * <p>需要路由的根本原因：任务定义在调度中心，但执行在别的机器上，
 * 而执行器是可以水平扩容的。有多个候选，就必须有选择规则。
 */
@Component
public class Router {

    /** 轮询计数。用 AtomicInteger 是因为调度循环可能多线程跑 */
    private final AtomicInteger roundRobin = new AtomicInteger(0);

    public enum Strategy {
        ROUND_ROBIN,
        RANDOM
    }

    public ExecutorNode pick(List<ExecutorNode> candidates, Strategy strategy) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        if (strategy == Strategy.RANDOM) {
            return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
        }
        int idx = Math.floorMod(roundRobin.getAndIncrement(), candidates.size());
        return candidates.get(idx);
    }

    public ExecutorNode pick(List<ExecutorNode> candidates) {
        return pick(candidates, Strategy.ROUND_ROBIN);
    }
}
