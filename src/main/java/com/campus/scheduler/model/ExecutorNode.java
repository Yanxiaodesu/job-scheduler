package com.campus.scheduler.model;

import lombok.Data;

import java.time.LocalDateTime;

/** 执行器注册信息，对应 executor_registry 表。 */
@Data
public class ExecutorNode {

    private Long id;
    private String appName;
    /** 形如 http://127.0.0.1:9001 */
    private String address;
    private LocalDateTime lastHeartbeat;
    /** 1=在线，0=离线 */
    private Integer status;
    private LocalDateTime createTime;
}
