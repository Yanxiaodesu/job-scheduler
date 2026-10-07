package com.campus.scheduler.core;

import com.campus.scheduler.model.ExecutorNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 路由策略测试。纯单元测试，不起 Spring。
 *
 * <p>路由本身很简单，但「多执行器时轮流分发」这件事必须真的成立 ——
 * 否则所有任务都压在一台机器上，扩容就白扩了。
 */
class RouterTest {

    private final Router router = new Router();

    private List<ExecutorNode> nodes(int n) {
        List<ExecutorNode> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ExecutorNode e = new ExecutorNode();
            e.setId((long) i);
            e.setAddress("http://127.0.0.1:" + (9000 + i));
            list.add(e);
        }
        return list;
    }

    @Test
    @DisplayName("轮询：三个执行器应当均匀轮流，不会全压在一台上")
    void roundRobinIsEven() {
        List<ExecutorNode> candidates = nodes(3);
        Map<Long, Integer> hits = new HashMap<>();

        int times = 300;
        for (int i = 0; i < times; i++) {
            ExecutorNode picked = router.pick(candidates, Router.Strategy.ROUND_ROBIN);
            hits.merge(picked.getId(), 1, Integer::sum);
        }

        assertEquals(3, hits.size(), "三个执行器都应该被选中过");
        for (Map.Entry<Long, Integer> e : hits.entrySet()) {
            assertEquals(times / 3, e.getValue(),
                    "轮询必须均匀：执行器 " + e.getKey() + " 被选中 " + e.getValue() + " 次");
        }
    }

    @Test
    @DisplayName("随机：多次选取应当覆盖到所有执行器")
    void randomCoversAll() {
        List<ExecutorNode> candidates = nodes(4);
        Map<Long, Integer> hits = new HashMap<>();
        for (int i = 0; i < 400; i++) {
            hits.merge(router.pick(candidates, Router.Strategy.RANDOM).getId(), 1, Integer::sum);
        }
        assertEquals(4, hits.size(), "随机策略也应该覆盖到所有候选");
    }

    @Test
    @DisplayName("没有候选时返回 null，而不是抛异常")
    void emptyReturnsNull() {
        // 这条很重要：调度循环里用 null 表示「没有可用执行器」，
        // 实例要留在 PENDING 等执行器上线，而不是标记失败
        assertNull(router.pick(List.of()));
        assertNull(router.pick(null));
    }

    @Test
    @DisplayName("单执行器时永远选它")
    void singleCandidate() {
        List<ExecutorNode> one = nodes(1);
        for (int i = 0; i < 10; i++) {
            assertEquals(0L, router.pick(one).getId());
        }
    }

    @Test
    @DisplayName("轮询计数溢出不应导致下标越界")
    void roundRobinHandlesOverflow() {
        // 用 Math.floorMod 而不是 % 就是为了这个：
        // AtomicInteger 溢出成负数时，% 会得到负下标直接崩
        List<ExecutorNode> candidates = nodes(3);
        for (int i = 0; i < 100; i++) {
            assertNotNull(router.pick(candidates));
        }
    }
}
