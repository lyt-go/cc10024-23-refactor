"""验证采集运行回执的构造、编号、提交、恢复和核对。

覆盖：
1. 同一交班内批次结束与首次恢复各自登记，编号前缀/计数字段/状态/来源不变；
2. 并发确认各批只让提交成功的回执占用正式运行编号；
3. 内存记录已建立、运行日志追加中断后的重试与核对，不产生第二份记录；
4. 运行历史能识别当初触发的是批次结束还是首次恢复；
5. 只中断一路持久化时，另一路照常提交，两路记录逐字段可比。
"""
from __future__ import annotations

import os
import sys
import threading
import json

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist
from app.store import store


def _clear_data() -> None:
    for sub in ["runs", "snapshots", "reports", "exports"]:
        p = persist.data_path(sub, "")
        if os.path.isdir(p):
            for name in os.listdir(p):
                os.remove(os.path.join(p, name))


class _BrokenWrite:
    """替换 append_jsonl：前 n 次写运行日志（run_history.jsonl）时抛错，模拟日志写入中断。"""

    def __init__(self, failures: int = 1):
        self.failures = failures
        self.calls = 0

    def __call__(self, sub, filename, record):
        if sub == "runs" and filename == "run_history.jsonl":
            self.calls += 1
            if self.failures > 0:
                self.failures -= 1
                raise OSError("simulated run log interruption")
        return _real_append(sub, filename, record)


_real_append = persist.append_jsonl


def _reload_receipt_module():
    """每个用例重建回执模块状态，模拟一次干净的进程/交班。"""
    from app.store import Store
    import app.services.run_receipt as rr

    store._tables = {name: [dict(row) for row in rows]
                     for name, rows in __import__("app.seed", fromlist=["SEED_ROWS"]).SEED_ROWS.items()}
    rr._COMMITTED.clear()
    rr._PENDING.clear()
    rr._FAILURES.clear()
    return rr


def test_batch_and_recovery_same_shift():
    """同一交班中两路来源各自登记，字段/动作保持既有契约。"""
    _clear_data()
    rr = _reload_receipt_module()
    from app.services.batch_queue import BatchQueueService
    from app.services.error_recover import ErrorRecoverService

    batch_svc = BatchQueueService()
    batch, _ = batch_svc.create_entry({
        "批次编号": "BATCH-SHIFT-1", "关联计划": "PLAN-SHIFT", "任务数量": 7})
    recovery_svc = ErrorRecoverService()
    recovery, _ = recovery_svc.create_entry({
        "任务编号": "REC-SHIFT-1", "关联计划": "PLAN-SHIFT",
        "失败步骤": "data_extract", "已重试次数": "2", "错误类型": "TimeoutError"})

    b_res, _ = batch_svc.run_action(batch["id"], "确认完成")
    r_res, _ = recovery_svc.run_action(recovery["id"], "确认恢复")
    assert b_res["status"] == "已完成" and r_res["status"] == "已恢复"

    batch_row = next(r for r in store.rows("run_history")
                     if str(r.get("记录编号", "")).startswith("RUN-BATCH-"))
    recovery_row = next(r for r in store.rows("run_history")
                        if str(r.get("记录编号", "")).startswith("RUN-RECOVER-"))

    # 编号前缀、计数字段、状态、动作维持原样
    assert batch_row["记录编号"] == f"RUN-BATCH-{batch_row['id']:04d}"
    assert batch_row["采集行数"] == "7"
    assert batch_row["status"] == "已完成" and batch_row["pending"] is False
    assert batch_row["执行日志"] == f"批次{batch['批次编号']}执行完成"
    assert recovery_row["记录编号"] == f"RUN-RECOVER-{recovery_row['id']:04d}"
    assert recovery_row["失败行数"] == "2"
    assert recovery_row["采集行数"] == "恢复重采"
    assert recovery_row["status"] == "运行中" and recovery_row["pending"] is True
    assert recovery_row["异常摘要"] == "TimeoutError"
    assert recovery_row["执行日志"] == f"从error_recover任务{recovery['任务编号']}恢复"
    assert batch_row["id"] != recovery_row["id"]

    logs = [r for r in persist.read_jsonl("runs", "run_history.jsonl")
            if r.get("action") in ("auto_create_from_batch", "auto_create_from_recovery")]
    by_action = {r["action"]: r for r in logs}
    assert by_action["auto_create_from_batch"]["source_module"] == "batch_queue"
    assert by_action["auto_create_from_recovery"]["source_module"] == "error_recover"
    assert by_action["auto_create_from_batch"]["entry_id"] == batch_row["id"]
    assert by_action["auto_create_from_recovery"]["entry_id"] == recovery_row["id"]

    # 运行历史能识别触发来源
    from app.services.run_history import RunHistoryService
    history = RunHistoryService()
    recent = {r["entry_id"]: r for r in history.get_recent_runs(50)}
    assert recent[batch_row["id"]]["trigger"] == "批次结束"
    assert recent[recovery_row["id"]]["trigger"] == "首次恢复"
    assert recent[batch_row["id"]]["source_module"] == "batch_queue"
    assert recent[recovery_row["id"]]["source_module"] == "error_recover"

    reconcile = history.reconcile_receipts()
    assert reconcile["memory_only"] == [] and reconcile["log_only"] == []
    assert reconcile["by_source"] == {"batch_queue": 1, "error_recover": 1}


def test_concurrent_confirm_unique_ids():
    """并发确认各批：只有日志提交成功的回执占用正式编号。"""
    _clear_data()
    rr = _reload_receipt_module()
    from app.services.batch_queue import BatchQueueService

    svc = BatchQueueService()
    batches = []
    for index in range(8):
        entry, _ = svc.create_entry({
            "批次编号": f"CONC-{index}", "关联计划": "PLAN-CONC", "任务数量": index + 1})
        batches.append(entry)

    barrier = threading.Barrier(len(batches))
    errors: list[BaseException] = []

    def confirm(entry):
        try:
            barrier.wait(timeout=5)
            svc.run_action(entry["id"], "确认完成")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=confirm, args=(b,)) for b in batches]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not errors, errors

    rows = [r for r in store.rows("run_history")
            if str(r.get("记录编号", "")).startswith("RUN-BATCH-")]
    assert len(rows) == len(batches)
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids)), f"运行编号重复: {ids}"

    logs = [r for r in persist.read_jsonl("runs", "run_history.jsonl")
            if r.get("action") == "auto_create_from_batch"]
    assert len(logs) == len(batches)
    assert len({r["entry_id"] for r in logs}) == len(batches)

    state = rr.collection_receipt_state()
    assert state["committed_count"] == len(batches)
    assert state["pending_count"] == 0 and state["failed_count"] == 0

    reconcile = rr.reconcile_collection_receipts()
    assert reconcile["matched_count"] == len(batches)
    assert reconcile["memory_only"] == [] and reconcile["log_only"] == []


def test_interrupted_log_retry_same_source():
    """内存记录已建立、日志追加中断：重试后只保留一份记录并核对通过。"""
    _clear_data()
    rr = _reload_receipt_module()
    from app.services.batch_queue import BatchQueueService

    svc = BatchQueueService()
    entry, _ = svc.create_entry({
        "批次编号": "INT-BATCH", "关联计划": "PLAN-INT", "任务数量": 3})

    broken = _BrokenWrite(failures=1)
    persist.append_jsonl = broken
    try:
        svc.run_action(entry["id"], "确认完成")
    finally:
        persist.append_jsonl = _real_append

    # 中断后：内存半成品已回滚，失败可查，没有占用正式编号
    rows = [r for r in store.rows("run_history")
            if str(r.get("记录编号", "")).startswith("RUN-BATCH-")]
    assert rows == [], f"中断后不应留下内存记录: {rows}"
    state = rr.collection_receipt_state()
    assert state["committed_count"] == 0
    assert state["failed_count"] == 1
    assert state["failed"][0]["source_module"] == "batch_queue"
    assert state["failed"][0]["trigger"] == "批次结束"

    # 同一入口重试：继续登记并核对，只有一条记录、一条日志
    svc.run_action(entry["id"], "确认完成")
    rows = [r for r in store.rows("run_history")
            if str(r.get("记录编号", "")).startswith("RUN-BATCH-")]
    logs = [r for r in persist.read_jsonl("runs", "run_history.jsonl")
            if r.get("action") == "auto_create_from_batch"]
    assert len(rows) == 1 and len(logs) == 1
    assert logs[0]["entry_id"] == rows[0]["id"]
    assert rr.collection_receipt_state()["failed_count"] == 0
    assert rr.reconcile_collection_receipts()["matched_count"] == 1


def test_interrupted_after_log_before_ack():
    """日志实际已落盘、但提交未确认：重试必须沿用原编号，不写第二份。"""
    _clear_data()
    rr = _reload_receipt_module()
    from app.services.error_recover import ErrorRecoverService

    svc = ErrorRecoverService()
    entry, _ = svc.create_entry({
        "任务编号": "ACK-REC", "关联计划": "PLAN-ACK", "失败步骤": "extract",
        "已重试次数": "1", "错误类型": "NetworkError"})

    class _LogThenCrash:
        """真实写运行日志，然后抛错，模拟写完后进程中断。"""

        def __call__(self, sub, filename, record):
            _real_append(sub, filename, record)
            if sub == "runs" and filename == "run_history.jsonl":
                raise OSError("interrupted after durable append")

    persist.append_jsonl = _LogThenCrash()
    try:
        svc.run_action(entry["id"], "确认恢复")
    finally:
        persist.append_jsonl = _real_append

    assert [r for r in store.rows("run_history")
            if str(r.get("记录编号", "")).startswith("RUN-RECOVER-")] == []
    logs_before = [r for r in persist.read_jsonl("runs", "run_history.jsonl")
                   if r.get("action") == "auto_create_from_recovery"]
    assert len(logs_before) == 1

    # 重试：采用日志里已经提交的编号补齐内存记录
    svc.run_action(entry["id"], "确认恢复")
    rows = [r for r in store.rows("run_history")
            if str(r.get("记录编号", "")).startswith("RUN-RECOVER-")]
    logs_after = [r for r in persist.read_jsonl("runs", "run_history.jsonl")
                  if r.get("action") == "auto_create_from_recovery"]
    assert len(rows) == 1, f"恢复引入第二份内存记录: {rows}"
    assert len(logs_after) == 1, f"恢复引入第二份日志: {logs_after}"
    assert rows[0]["id"] == logs_after[0]["entry_id"] == logs_before[0]["entry_id"]
    assert rr.reconcile_collection_receipts()["matched_count"] == 1


def test_interrupt_one_path_other_completes():
    """同时确认批次与恢复，只中断一路：两路记录可分别比较字段与动作。"""
    _clear_data()
    rr = _reload_receipt_module()
    from app.services.batch_queue import BatchQueueService
    from app.services.error_recover import ErrorRecoverService

    batch_svc = BatchQueueService()
    batch, _ = batch_svc.create_entry({
        "批次编号": "SPLIT-BATCH", "关联计划": "PLAN-SPLIT", "任务数量": 4})
    recovery_svc = ErrorRecoverService()
    recovery, _ = recovery_svc.create_entry({
        "任务编号": "SPLIT-REC", "关联计划": "PLAN-SPLIT", "失败步骤": "extract",
        "已重试次数": "3", "错误类型": "ParseError"})

    broken = _BrokenWrite(failures=1)
    persist.append_jsonl = broken
    try:
        batch_svc.run_action(batch["id"], "确认完成")       # 这一路中断
        recovery_svc.run_action(recovery["id"], "确认恢复")  # 这一路正常
    finally:
        persist.append_jsonl = _real_append

    batch_rows = [r for r in store.rows("run_history")
                  if str(r.get("记录编号", "")).startswith("RUN-BATCH-")]
    recovery_rows = [r for r in store.rows("run_history")
                     if str(r.get("记录编号", "")).startswith("RUN-RECOVER-")]
    assert batch_rows == [], "中断的批次不应留下内存记录"
    assert len(recovery_rows) == 1, "另一路应照常提交"
    assert recovery_rows[0]["status"] == "运行中"

    state = rr.collection_receipt_state()
    assert state["committed_count"] == 1
    assert state["failed_count"] == 1
    assert state["failed"][0]["source_module"] == "batch_queue"
    assert state["committed"][0]["source_module"] == "error_recover"
    assert state["committed"][0]["trigger"] == "首次恢复"

    # 重试中断的一路
    batch_svc.run_action(batch["id"], "确认完成")
    batch_rows = [r for r in store.rows("run_history")
                  if str(r.get("记录编号", "")).startswith("RUN-BATCH-")]
    assert len(batch_rows) == 1
    assert batch_rows[0]["status"] == "已完成"
    assert batch_rows[0]["采集行数"] == "4"

    rows = batch_rows + recovery_rows
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids)), f"两路编号冲突: {ids}"
    logs = [r for r in persist.read_jsonl("runs", "run_history.jsonl")
            if r.get("action") in ("auto_create_from_batch", "auto_create_from_recovery")]
    assert len(logs) == 2
    assert {log["action"] for log in logs} == {
        "auto_create_from_batch", "auto_create_from_recovery"}
    assert {log["source_module"] for log in logs} == {"batch_queue", "error_recover"}

    # 逐项比较两路记录与既有接口字段/动作
    for row, action, module, trigger, status in (
        (batch_rows[0], "auto_create_from_batch", "batch_queue", "批次结束", "已完成"),
        (recovery_rows[0], "auto_create_from_recovery", "error_recover", "首次恢复", "运行中"),
    ):
        assert row["status"] == status
        log = next(log for log in logs if log["entry_id"] == row["id"])
        assert log["action"] == action
        assert log["source_module"] == module
        assert log["status"] == row["status"]
        history = next(
            r for r in __import__("app.services.run_history", fromlist=["RunHistoryService"])
            .RunHistoryService().get_recent_runs(50) if r.get("entry_id") == row["id"])
        assert history["trigger"] == trigger

    reconcile = rr.reconcile_collection_receipts()
    assert reconcile["matched_count"] == 2
    assert reconcile["memory_only"] == [] and reconcile["log_only"] == []
    assert reconcile["by_source"] == {"batch_queue": 1, "error_recover": 1}


def test_partial_tail_line_recovered():
    """日志末尾残留半行时，登记会把完整记录接在后面，历史仍可逐条读取。"""
    _clear_data()
    rr = _reload_receipt_module()
    from app.services.batch_queue import BatchQueueService

    svc = BatchQueueService()
    entry, _ = svc.create_entry({
        "批次编号": "TAIL-BATCH", "关联计划": "PLAN-TAIL", "任务数量": 2})

    # 手工制造一次写到一半的中断
    path = persist.data_path("runs", "run_history.jsonl")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write('{"module": "run_history", "entry_id": 4, "act')

    svc.run_action(entry["id"], "确认完成")
    raw_lines = [line for line in open(path, encoding="utf-8").read().splitlines() if line]
    assert all(line.endswith("}") for line in raw_lines), raw_lines
    logs = persist.read_jsonl("runs", "run_history.jsonl")
    assert len(logs) == 1 and logs[0]["action"] == "auto_create_from_batch"

    rows = [r for r in store.rows("run_history")
            if str(r.get("记录编号", "")).startswith("RUN-BATCH-")]
    assert len(rows) == 1
    reconcile = rr.reconcile_collection_receipts()
    assert reconcile["matched_count"] == 1
    assert reconcile["memory_only"] == [] and reconcile["log_only"] == []


if __name__ == "__main__":
    test_batch_and_recovery_same_shift()
    print("PASS: test_batch_and_recovery_same_shift")
    test_concurrent_confirm_unique_ids()
    print("PASS: test_concurrent_confirm_unique_ids")
    test_interrupted_log_retry_same_source()
    print("PASS: test_interrupted_log_retry_same_source")
    test_interrupted_after_log_before_ack()
    print("PASS: test_interrupted_after_log_before_ack")
    test_interrupt_one_path_other_completes()
    print("PASS: test_interrupt_one_path_other_completes")
    test_partial_tail_line_recovered()
    print("PASS: test_partial_tail_line_recovered")
    print("\nALL RECEIPT TESTS PASSED")
