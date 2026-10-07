package com.campus.scheduler.core;

import com.campus.scheduler.config.SchedulerProperties;
import com.campus.scheduler.model.ExecutorNode;
import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.model.JobInstance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 把任务派发给执行器。
 *
 * <p>用普通 HTTP 而不是 xxl-job 那种 Netty 内嵌 RPC —— 通信层不是这个项目的重点，
 * 把时间花在造 RPC 上是本末倒置。<b>但代价要能说清楚</b>：
 * HTTP 比长连接 RPC 多一次握手开销；不过调度频率是秒级，这点开销可以忽略。
 *
 * <p>这里算「触发式派发」：调度中心主动 push。另一种设计是执行器主动 pull
 * （轮询任务表），pull 的好处是执行器不用暴露端口，坏处是触发有延迟且 DB 压力大。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class Dispatcher {

    private final RestClient client = RestClient.builder().build();

    /** 派发时要告诉执行器「结果回调到哪里」，这样执行器不用自己配 Admin 地址 */
    private final SchedulerProperties props;

    /**
     * 派发一个任务实例。
     *
     * @return true 表示执行器已接收（2xx）；false 表示网络失败或执行器报错
     */
    public boolean dispatch(ExecutorNode node, JobInfo job, JobInstance instance) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("instanceId", instance.getId());
        payload.put("jobId", job.getId());
        payload.put("jobName", job.getJobName());
        payload.put("handlerType", job.getHandlerType());
        payload.put("handlerValue", job.getHandlerValue());
        payload.put("param", job.getParam());
        payload.put("timeoutSec", job.getTimeoutSec());
        payload.put("callbackUrl", props.getCallbackUrl());
        // ★ 带上重试次数：执行器用它做去重键的一部分。
        //   如果只用 instanceId 去重，「失败重试」会被执行器误判成重复派发而直接忽略 ——
        //   重试会静默失效。带上 retryCount 后，每次重试都是一个新键。
        payload.put("retryCount", instance.getRetryCount());

        String url = node.getAddress() + "/executor/run";
        try {
            client.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(payload)
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (Exception e) {
            // 派发失败**不能吞掉** —— 调用方要靠返回值决定是否重试/转移
            log.warn("派发失败 node={} job={} instance={} err={}",
                    node.getAddress(), job.getJobName(), instance.getId(), e.getMessage());
            return false;
        }
    }
}
