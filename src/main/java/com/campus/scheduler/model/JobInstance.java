package com.campus.scheduler.model;

import lombok.Data;

import java.time.LocalDateTime;

/** 任务实例：每次「触发」产生一行，对应 job_instance 表。 */
@Data
public class JobInstance {

    /** 状态枚举。用常量而不是魔法数字，方便日志和排查 */
    public static final int PENDING = 0;   // 待执行
    public static final int RUNNING = 1;   // 执行中
    public static final int SUCCESS = 2;   // 成功
    public static final int FAILED = 3;    // 失败
    public static final int TIMEOUT = 4;   // 超时
    public static final int SKIPPED = 5;   // 已跳过（misfire 且策略为跳过）

    private Long id;
    private Long jobId;
    private Long executorId;

    /** 计划触发时刻 */
    private LocalDateTime triggerTime;

    /**
     * ★ "jobId:触发时刻"，本表上有 UNIQUE KEY。
     *
     * <p>这是「恰好一次」的第二层保险：即使调度循环因为任何原因
     * 对同一个触发时刻派发了两次，数据库也只允许落一条实例。
     */
    private String instanceKey;

    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Integer status;
    private Integer retryCount;
    private String resultMsg;

    private LocalDateTime createTime;
}
