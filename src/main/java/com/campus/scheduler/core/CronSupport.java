package com.campus.scheduler.core;

import org.springframework.scheduling.support.CronExpression;

import java.time.LocalDateTime;

/**
 * cron 表达式解析。
 *
 * <p><b>刻意不自己手写解析器</b>：cron 的边界情况很多（月份天数、闰年、跨年），
 * 手写只会写出一堆 bug，而且这个项目的价值不在「解析字符串」。
 * Spring 自带的 {@link CronExpression} 是 6 段式（秒 分 时 日 月 周），够用了。
 *
 * <p>面试如果被问「cron 怎么解析的」，如实说「用 Spring 自带的」，
 * 然后讲清楚 {@link #next} 是「从某个时刻往后算下一次」，不要硬吹自己实现。
 */
public final class CronSupport {

    private CronSupport() {
    }

    /** 从 from 之后算下一次触发时刻。表达式非法时抛 IllegalArgumentException。 */
    public static LocalDateTime next(String cron, LocalDateTime from) {
        return parse(cron).next(from);
    }

    public static LocalDateTime next(String cron) {
        return next(cron, LocalDateTime.now());
    }

    public static CronExpression parse(String cron) {
        if (cron == null || cron.isBlank()) {
            throw new IllegalArgumentException("cron 表达式不能为空");
        }
        try {
            return CronExpression.parse(cron.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("cron 表达式非法：" + cron + "（" + e.getMessage() + "）");
        }
    }

    public static boolean isValid(String cron) {
        try {
            parse(cron);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
