package com.campus.scheduler.executor;

/**
 * 内置任务接口。
 *
 * <p>一个任务要么是「内置 handler」（实现这个接口、注册成 Spring Bean），
 * 要么是「HTTP 回调」（把请求转发到业务方自己的 URL）。
 *
 * <p>两种都留着是有原因的：内置的适合跟调度平台一起部署的运维脚本；
 * HTTP 回调适合业务方不想引调度 SDK 的情况。xxl-job 更激进，
 * 支持在线写 Java/Shell/Python 脚本（GLUE），那个是另一个量级的复杂度，这里不做。
 */
public interface JobHandler {

    /** handler 名，任务配置里 handler_value 写的就是它 */
    String name();

    /**
     * 执行任务。
     *
     * @param instanceId 实例 id，业务可以用它做幂等
     * @param param      可选参数
     * @return 执行结果摘要，会写进 job_instance.result_msg
     */
    String execute(Long instanceId, String param) throws Exception;
}
