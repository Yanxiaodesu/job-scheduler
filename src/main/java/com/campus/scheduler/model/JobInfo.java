package com.campus.scheduler.model;

import lombok.Data;

import java.time.LocalDateTime;

/** 任务定义，对应 job_info 表。 */
@Data
public class JobInfo {

    private Long id;
    private String jobName;
    private String cronExpr;

    /** 1=内置 handler，2=HTTP 回调 */
    private Integer handlerType;
    /** 内置 handler 名，或回调 URL */
    private String handlerValue;
    /** 传给 handler 的参数。比如 sleep handler 用它指定睡几秒 */
    private String param;

    private Integer timeoutSec;
    private Integer maxRetry;
    /** 1=补跑一次，2=跳过 */
    private Integer misfireStrategy;
    /** 0=停用，1=启用 */
    private Integer status;

    /**
     * ★ 调度循环唯一的扫描依据：WHERE status=1 AND next_trigger_time &lt;= NOW()
     *
     * <p>它同时还是「抢占」的载体 —— 推进它的时候带上旧值做条件，
     * 只有一个人的 UPDATE 能成功。见 {@code JobInfoRepo#advanceTriggerTime}。
     */
    private LocalDateTime nextTriggerTime;

    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
