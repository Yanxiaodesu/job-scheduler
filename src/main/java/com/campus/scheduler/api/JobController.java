package com.campus.scheduler.api;

import com.campus.scheduler.core.CronSupport;
import com.campus.scheduler.core.InstanceDispatcher;
import com.campus.scheduler.model.JobInfo;
import com.campus.scheduler.model.JobInstance;
import com.campus.scheduler.repo.JobInfoRepo;
import com.campus.scheduler.repo.JobInstanceRepo;
import com.campus.scheduler.repo.JobLogRepo;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/** 任务管理 API。 */
@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobInfoRepo jobRepo;
    private final JobInstanceRepo instanceRepo;
    private final JobLogRepo logRepo;
    private final InstanceDispatcher instanceDispatcher;

    @Data
    public static class JobForm {
        private String jobName;
        /** 6 段式：秒 分 时 日 月 周 */
        private String cronExpr;
        /** 1=内置 handler，2=HTTP 回调 */
        private Integer handlerType = 1;
        private String handlerValue;
        /** 传给 handler 的参数，例如 sleep 的秒数 */
        private String param;
        private Integer timeoutSec = 30;
        private Integer maxRetry = 0;
        /** 1=补跑一次，2=跳过 */
        private Integer misfireStrategy = 2;
        private Integer status = 1;
    }

    @GetMapping
    public List<JobInfo> list() {
        return jobRepo.findAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> get(@PathVariable Long id) {
        JobInfo j = jobRepo.findById(id);
        return j == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(j);
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody JobForm form) {
        // 先校验，别让非法 cron 进库 —— 否则调度循环每轮都抛异常
        if (!CronSupport.isValid(form.getCronExpr())) {
            return ResponseEntity.badRequest().body(
                    Map.of("error", "cron 表达式非法：" + form.getCronExpr()
                            + "（需要 6 段式：秒 分 时 日 月 周）"));
        }
        if (jobRepo.findByName(form.getJobName()) != null) {
            return ResponseEntity.badRequest().body(Map.of("error", "任务名已存在：" + form.getJobName()));
        }

        JobInfo j = new JobInfo();
        j.setJobName(form.getJobName());
        j.setCronExpr(form.getCronExpr());
        j.setHandlerType(form.getHandlerType());
        j.setHandlerValue(form.getHandlerValue());
        j.setParam(form.getParam());
        j.setTimeoutSec(form.getTimeoutSec());
        j.setMaxRetry(form.getMaxRetry());
        j.setMisfireStrategy(form.getMisfireStrategy());
        j.setStatus(form.getStatus());
        // ★ 新建任务时就把下次触发时间算好，否则它永远不会被扫到
        j.setNextTriggerTime(CronSupport.next(form.getCronExpr()));

        Long id = jobRepo.insert(j);
        log.info("创建任务 id={} name={} cron={} next={}", id, j.getJobName(), j.getCronExpr(),
                j.getNextTriggerTime());
        return ResponseEntity.ok(Map.of("id", id, "nextTriggerTime", j.getNextTriggerTime().toString()));
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable Long id, @RequestBody JobForm form) {
        JobInfo j = jobRepo.findById(id);
        if (j == null) {
            return ResponseEntity.notFound().build();
        }
        if (!CronSupport.isValid(form.getCronExpr())) {
            return ResponseEntity.badRequest().body(Map.of("error", "cron 表达式非法"));
        }
        j.setCronExpr(form.getCronExpr());
        j.setHandlerType(form.getHandlerType());
        j.setHandlerValue(form.getHandlerValue());
        j.setParam(form.getParam());
        j.setTimeoutSec(form.getTimeoutSec());
        j.setMaxRetry(form.getMaxRetry());
        j.setMisfireStrategy(form.getMisfireStrategy());
        j.setStatus(form.getStatus());
        // 改了 cron 就重算下次触发时间
        j.setNextTriggerTime(CronSupport.next(form.getCronExpr()));
        jobRepo.update(j);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    @PostMapping("/{id}/enable")
    public ResponseEntity<?> enable(@PathVariable Long id) {
        JobInfo j = jobRepo.findById(id);
        if (j == null) {
            return ResponseEntity.notFound().build();
        }
        // 重新启用时要重算触发时间：停用期间的时间点已经过去了，
        // 不重算的话它一启用就会被判定为 misfire
        jobRepo.setStatus(id, 1);
        j.setStatus(1);
        j.setNextTriggerTime(CronSupport.next(j.getCronExpr()));
        jobRepo.update(j);
        return ResponseEntity.ok(Map.of("ok", true, "nextTriggerTime", j.getNextTriggerTime().toString()));
    }

    @PostMapping("/{id}/disable")
    public ResponseEntity<?> disable(@PathVariable Long id) {
        jobRepo.setStatus(id, 0);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable Long id) {
        jobRepo.delete(id);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    /**
     * 手动触发一次（不影响 cron 排期）。
     *
     * <p>调试时非常有用：不用等到下一次 cron 到点，点上就能看效果。
     *
     * <p>两点设计：
     * <ul>
     *   <li><b>直接派发，不改 next_trigger_time</b> —— 手动触发不该打乱正常排期</li>
     *   <li><b>立即派发，不等重派扫描</b> —— 早期版本是建完实例就返回，
     *       依赖 5 秒一次的重派循环去发。结果是点一下要等最多 5 秒才动，
     *       批量触发上千个任务时吞吐只有 4.5 个/秒。改成当场派发后就正常了。</li>
     * </ul>
     */
    @PostMapping("/{id}/trigger")
    public ResponseEntity<?> triggerNow(@PathVariable Long id) {
        JobInfo j = jobRepo.findById(id);
        if (j == null) {
            return ResponseEntity.notFound().build();
        }
        LocalDateTime now = LocalDateTime.now();
        // key 里带纳秒，保证快速连续触发不会撞唯一键
        String key = j.getId() + ":manual:" + now;
        Long instId = instanceRepo.tryCreate(j.getId(), now, key);
        if (instId == null) {
            return ResponseEntity.ok(Map.of("ok", false, "msg", "重复触发"));
        }

        JobInstance inst = instanceRepo.findById(instId);
        boolean dispatched = instanceDispatcher.dispatchOne(j, inst);
        return ResponseEntity.ok(Map.of(
                "ok", true,
                "instanceId", instId,
                "dispatched", dispatched,
                "msg", dispatched ? "已派发" : "已创建，但没有可用执行器，等待上线后重派"));
    }

    @GetMapping("/{id}/instances")
    public List<JobInstance> instances(@PathVariable Long id,
                                       @RequestParam(defaultValue = "20") int limit) {
        return instanceRepo.findByJob(id, Math.min(limit, 200));
    }

    @GetMapping("/instances/{instanceId}/logs")
    public List<Map<String, Object>> logs(@PathVariable Long instanceId) {
        return logRepo.findByInstance(instanceId);
    }
}
