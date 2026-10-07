#!/usr/bin/env python3
"""压测 / 验证脚本 —— 把 README 里那些数字变成「跑一下就能看到」。

## 为什么要有这个脚本

README 里写了「3 个 Admin 并发 82 个实例 0 重复」「调度精度 P50 108ms」这类数字。
如果这些数字只能靠作者手动跑出来，那它就只是一句自述，评审者无法验证。

这个脚本让任何人都能在自己机器上复现同一套结论。

## 用法

    # 先起好集群（至少 1 个 admin + 1 个 executor）
    python scripts/benchmark.py
    python scripts/benchmark.py --duration 60 --throughput-jobs 500

## 三个测试

    1. 并发零重复 —— 最能说明问题的一项
    2. 调度精度   —— 实际派发 - 计划触发
    3. 调度吞吐   —— 上千任务同时到点，一轮扫完要多久

只用标准库，不需要装任何依赖。
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request

TIMEOUT = 30


# ============================================================
# HTTP 小工具
# ============================================================

def call(method: str, url: str, body: dict | None = None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        raw = resp.read().decode("utf-8")
        return resp.status, (json.loads(raw) if raw else None)


def get(url):
    return call("GET", url)


def post(url, body=None):
    return call("POST", url, body)


def die(msg: str):
    print(f"\n  [X] {msg}\n")
    sys.exit(1)


# ============================================================
# 集群检查
# ============================================================

def probe(port: int, role: str) -> dict | None:
    try:
        _, m = get(f"http://127.0.0.1:{port}/api/metrics")
        if role and m.get("role") != role:
            return None
        return m
    except Exception:
        return None


def discover(admin_ports, executor_ports):
    admins, executors = [], []
    for p in admin_ports:
        m = probe(p, None)
        if m and m.get("role") in ("admin", "both"):
            admins.append((p, m))
    for p in executor_ports:
        m = probe(p, None)
        if m and m.get("role") in ("executor", "both"):
            executors.append((p, m))
    return admins, executors


# ============================================================
# 测试 1：并发零重复
# ============================================================

def test_no_duplicate(admin: str, duration: int, cron: str) -> dict:
    """建一个高频任务，跑一段时间，看有没有重复实例。

    判定标准：同一 instance_key 只允许有一条记录。
    同时看各 Admin 的 lostTheRace 计数 —— 那个数字 > 0 才说明真的发生了
    并发碰撞，否则「0 重复」可能只是碰巧没撞上。
    """
    print(f"\n{'=' * 68}")
    print(f"  测试 1：并发零重复")
    print(f"{'=' * 68}")

    call("DELETE", f"{admin}/api/instances/reset")
    # 清掉旧任务，避免干扰
    _, jobs = get(f"{admin}/api/jobs")
    for j in jobs:
        call("DELETE", f"{admin}/api/jobs/{j['id']}")

    _, created = post(f"{admin}/api/jobs", {
        "jobName": f"bench-dup-{int(time.time())}",
        "cronExpr": cron,
        "handlerType": 1,
        "handlerValue": "printTime",
        "timeoutSec": 10,
        "maxRetry": 0,
        "misfireStrategy": 2,
        "status": 1,
    })
    print(f"  已建任务 cron='{cron}'，采样 {duration} 秒 ...")

    # 记录起始的 lostTheRace，只统计本次的增量
    before = {}
    for p in args.admin_ports:
        m = probe(p, None)
        if m:
            before[p] = m["scheduler"]["lostTheRace"]

    t0 = time.time()
    while time.time() - t0 < duration:
        time.sleep(5)
        elapsed = int(time.time() - t0)
        _, d = get(f"{admin}/api/instances/duplicates")
        print(f"    +{elapsed:>3}s  实例 {d['total']:>4}  重复 {d['duplicateKeys']}", flush=True)

    _, d = get(f"{admin}/api/instances/duplicates")
    lost_total = 0
    for p in args.admin_ports:
        m = probe(p, None)
        if m:
            lost_total += m["scheduler"]["lostTheRace"] - before.get(p, 0)

    result = {
        "total": d["total"],
        "duplicates": d["duplicateKeys"],
        "lostTheRace": lost_total,
        "ok": d["duplicateKeys"] == 0,
    }

    print(f"\n  实例总数      {result['total']}")
    print(f"  重复数        {result['duplicates']}   {'OK' if result['ok'] else 'FAIL'}")
    print(f"  抢输次数      {result['lostTheRace']}   "
          f"{'（真的发生了并发碰撞，防护被触发）' if lost_total > 0 else '（没撞上，说服力不足）'}")
    if result["total"] > 0 and lost_total == 0:
        print("  [注意] 抢输=0 说明多个 Admin 的扫描相位错开了，本次没真正碰撞。")
        print("         想制造碰撞请缩短 cron 间隔或加大扫描频率（SCAN_INTERVAL_MS）")
    return result


# ============================================================
# 测试 2：调度精度
# ============================================================

def test_precision(admin: str, duration: int) -> dict:
    print(f"\n{'=' * 68}")
    print(f"  测试 2：调度精度（实际派发 - 计划触发）")
    print(f"{'=' * 68}")

    call("DELETE", f"{admin}/api/instances/reset")
    _, jobs = get(f"{admin}/api/jobs")
    for j in jobs:
        call("DELETE", f"{admin}/api/jobs/{j['id']}")

    post(f"{admin}/api/jobs", {
        "jobName": f"bench-precision-{int(time.time())}",
        "cronExpr": "* * * * * *",
        "handlerType": 1,
        "handlerValue": "printTime",
        "timeoutSec": 10,
        "maxRetry": 0,
        "misfireStrategy": 2,
        "status": 1,
    })
    print(f"  已建每秒触发的任务，采样 {duration} 秒 ...")
    time.sleep(duration)

    _, p = get(f"{admin}/api/instances/precision")
    print(f"\n  样本数   {p.get('samples')}")
    print(f"  平均     {p.get('avgMs')} ms")
    print(f"  P50      {p.get('p50Ms')} ms")
    print(f"  P95      {p.get('p95Ms')} ms")
    print(f"  P99      {p.get('p99Ms')} ms")
    print(f"  最大     {p.get('maxMs')} ms  （最小 {p.get('minMs')} ms）")
    print(f"\n  提示：轮询式扫描的平均延迟 ≈ 扫描间隔的一半。")
    print(f"        把 SCAN_INTERVAL_MS 从 1000 改成 200 再跑一次，能看到明显差异。")
    return p


# ============================================================
# 测试 3：调度吞吐
# ============================================================

def test_throughput(admin: str, n_jobs: int) -> dict:
    """建 N 个任务让它们同时到点，测调度一轮要多久。

    这是「任务量上来之后会不会卡住」的直接答案。
    扫描循环一次最多处理 SCAN_BATCH(200) 条，所以任务多了必然要多轮 ——
    这个测试就是量出来「多轮要多久」。
    """
    print(f"\n{'=' * 68}")
    print(f"  测试 3：调度吞吐（{n_jobs} 个任务同时到点）")
    print(f"{'=' * 68}")

    call("DELETE", f"{admin}/api/instances/reset")
    _, jobs = get(f"{admin}/api/jobs")
    for j in jobs:
        call("DELETE", f"{admin}/api/jobs/{j['id']}")

    print(f"  正在创建 {n_jobs} 个任务 ...")
    t0 = time.time()
    for i in range(n_jobs):
        post(f"{admin}/api/jobs", {
            "jobName": f"bench-load-{i}-{int(time.time())}",
            # 每年一次 → 不会被自动触发，保证是「同时到点」的受控场景
            "cronExpr": "0 0 0 1 1 *",
            "handlerType": 1,
            "handlerValue": "printTime",
            "timeoutSec": 10,
            "maxRetry": 0,
            "misfireStrategy": 2,
            "status": 1,
        })
    create_sec = time.time() - t0
    print(f"  创建完成，用时 {create_sec:.1f}s（{n_jobs / create_sec:.0f} 个/秒）")

    # 让它们全部在同一秒到点
    print(f"  触发全部任务，测量调度完成时间 ...")
    call("DELETE", f"{admin}/api/instances/reset")
    t0 = time.time()
    _, jobs = get(f"{admin}/api/jobs")
    for j in jobs:
        call("POST", f"{admin}/api/jobs/{j['id']}/trigger")
    trigger_sec = time.time() - t0

    # 等执行完成
    deadline = time.time() + 180
    while time.time() < deadline:
        time.sleep(2)
        _, m = get(f"{admin}/api/metrics")
        inst = m["instances"]
        pending = inst["pending"] + inst["running"]
        if pending == 0:
            break

    total_sec = time.time() - t0
    _, m = get(f"{admin}/api/metrics")
    _, d = get(f"{admin}/api/instances/duplicates")

    result = {
        "jobs": n_jobs,
        "createSec": round(create_sec, 1),
        "totalSec": round(total_sec, 1),
        "perSec": round(n_jobs / total_sec, 1) if total_sec > 0 else 0,
        "duplicates": d["duplicateKeys"],
        "success": m["instances"]["success"],
        "failed": m["instances"]["failed"],
    }

    print(f"\n  任务数        {n_jobs}")
    print(f"  创建耗时      {create_sec:.1f}s")
    print(f"  投递耗时      {trigger_sec:.1f}s")
    print(f"  调度+执行     {total_sec:.1f}s")
    print(f"  吞吐          {result['perSec']} 个/秒")
    print(f"  成功/失败     {result['success']} / {result['failed']}")
    print(f"  重复          {result['duplicates']}   {'OK' if result['duplicates'] == 0 else 'FAIL'}")
    print(f"\n  说明：扫描循环每轮最多处理 200 条（SCAN_BATCH），")
    print(f"        {n_jobs} 个任务需要 {(n_jobs + 199) // 200} 轮；"
          f"每轮耗时取决于扫描间隔。")
    return result


# ============================================================
# 主流程
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="调度平台压测 / 验证")
    ap.add_argument("--admin", type=int, default=8080, help="主 Admin 端口")
    ap.add_argument("--admin-ports", type=str, default="8080,8081,8082",
                    help="用于统计抢输次数的所有 Admin 端口")
    ap.add_argument("--executors", type=str, default="9001,9002", help="执行器端口")
    ap.add_argument("--duration", type=int, default=45, help="零重复测试时长（秒）")
    ap.add_argument("--precision-duration", type=int, default=60, help="精度采样时长（秒）")
    ap.add_argument("--throughput-jobs", type=int, default=300, help="吞吐测试任务数")
    ap.add_argument("--skip-throughput", action="store_true")
    ap.add_argument("--cron", type=str, default="* * * * * *",
                    help="零重复测试用的 cron，默认每秒一次（更容易撞上）")
    global args
    args = ap.parse_args()

    admin = f"http://127.0.0.1:{args.admin}"
    args.admin_ports = [int(x) for x in args.admin_ports.split(",")]
    executor_ports = [int(x) for x in args.executors.split(",")]

    print(f"\n{'=' * 68}")
    print(f"  分布式定时任务调度平台 —— 压测 / 验证")
    print(f"{'=' * 68}")

    try:
        _, health = get(f"{admin}/api/metrics")
    except Exception as e:
        die(f"连不上 {admin}：{e}\n      请先启动集群：scripts\\run.cmd admin 8080")

    admins, executors = discover(args.admin_ports, executor_ports)
    print(f"\n  集群状态")
    print(f"    Admin 实例   {len(admins)} 个  {[p for p, _ in admins]}")
    print(f"    执行器       {len(executors)} 个  {[p for p, _ in executors]}")
    print(f"    扫描间隔     {health.get('scanIntervalMs', '?')} ms")

    if not executors:
        die("没有可用的执行器。请先启动：scripts\\run.cmd executor 9001")
    if len(admins) < 2:
        print(f"\n  [注意] 只有 {len(admins)} 个 Admin 实例，无法验证「多实例并发不重复」。")
        print(f"         建议再起两个：scripts\\run.cmd admin 8081 / 8082")

    results = {}
    results["duplicate"] = test_no_duplicate(admin, args.duration, args.cron)
    results["precision"] = test_precision(admin, args.precision_duration)
    if not args.skip_throughput:
        results["throughput"] = test_throughput(admin, args.throughput_jobs)

    print(f"\n{'=' * 68}")
    print(f"  汇总")
    print(f"{'=' * 68}")
    d = results["duplicate"]
    p = results["precision"]
    print(f"  零重复        {d['total']} 个实例 / {d['duplicates']} 重复 "
          f"（抢输 {d['lostTheRace']} 次）  {'通过' if d['ok'] else '不通过'}")
    print(f"  调度精度      P50 {p.get('p50Ms')}ms / P95 {p.get('p95Ms')}ms")
    if "throughput" in results:
        t = results["throughput"]
        print(f"  调度吞吐      {t['perSec']} 个/秒（{t['jobs']} 个任务）")
    print()

    if not d["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
