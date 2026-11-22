"""采集运行回执：批次结束与错误恢复共用的一套构造、编号和提交规则。

两个来源只在回执内容上有差异（编号前缀、计数字段、状态、动作与来源标记），
编号分配、内存登记、运行日志追加、失败回滚和重试核对统一在这里完成：

- 并发登记共用一把锁串行化，运行编号不会重复；
- 只有运行日志追加成功的回执才算提交成功、才占用正式运行编号；
- 追加失败时回滚内存里的半成品，不留下无法核对的记录；
- 重试同一来源时先核对运行日志：已经落盘的回执直接沿用，不再写第二条。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable

from app.store import store
from app import persistence as persist

RUN_LOG_MODULE = "run_history"
RUN_LOG_ENTITY = "运行记录"
RUN_LOG_FILE = "run_history.jsonl"


@dataclass(frozen=True)
class ReceiptSpec:
    """一种采集运行回执的固定业务规则。"""

    source_module: str
    prefix: str
    action: str
    trigger: str
    build_record: Callable[[dict[str, Any], int, str], dict[str, Any]]


def _now() -> str:
    return persist.now_iso()


def _batch_record(entry: dict[str, Any], run_id: int, started_at: str) -> dict[str, Any]:
    """批次收尾回执：沿用 RUN-BATCH 前缀、任务计数字段、已完成状态。"""
    finished_at = _now()
    return {
        "id": run_id,
        "记录编号": f"RUN-BATCH-{run_id:04d}",
        "关联计划": entry.get("关联计划", ""),
        "开始时间": started_at,
        "结束时间": finished_at,
        "采集行数": str(entry.get("任务数量", 0)),
        "失败行数": str(entry.get("失败数", 0)),
        "执行日志": f"批次{entry.get('批次编号')}执行完成",
        "异常摘要": "",
        "status": "已完成",
        "pending": False,
        "abnormal": False,
        "version": "",
        "updated_at": finished_at,
    }


def _recovery_record(entry: dict[str, Any], run_id: int, started_at: str) -> dict[str, Any]:
    """首次恢复回执：沿用 RUN-RECOVER 前缀、重试计数字段、运行中状态。"""
    return {
        "id": run_id,
        "记录编号": f"RUN-RECOVER-{run_id:04d}",
        "关联计划": entry.get("关联计划", ""),
        "开始时间": started_at,
        "结束时间": "",
        "采集行数": "恢复重采",
        "失败行数": entry.get("已重试次数", "0"),
        "执行日志": f"从error_recover任务{entry.get('任务编号')}恢复",
        "异常摘要": entry.get("错误类型", ""),
        "status": "运行中",
        "pending": True,
        "abnormal": False,
        "version": "",
        "updated_at": _now(),
    }


BATCH_RECEIPT = ReceiptSpec(
    source_module="batch_queue",
    prefix="RUN-BATCH",
    action="auto_create_from_batch",
    trigger="批次结束",
    build_record=_batch_record,
)
RECOVERY_RECEIPT = ReceiptSpec(
    source_module="error_recover",
    prefix="RUN-RECOVER",
    action="auto_create_from_recovery",
    trigger="首次恢复",
    build_record=_recovery_record,
)
RECEIPT_SPECS = (BATCH_RECEIPT, RECOVERY_RECEIPT)


# 已提交成功的回执：key 为 (来源模块, 来源记录id)
_LOCK = threading.RLock()
_COMMITTED: dict[tuple[str, int], dict[str, Any]] = {}
# 已建立内存记录、但日志尚未确认写入的回执
_PENDING: dict[tuple[str, int], dict[str, Any]] = {}
# 各来源最近一次提交失败的信息，供运营核对
_FAILURES: dict[tuple[str, int], dict[str, Any]] = {}


def _receipt_key(spec: ReceiptSpec, entry: dict[str, Any]) -> tuple[str, int]:
    return spec.source_module, int(entry.get("id") or 0)


def _find_memory_row(run_id: int) -> dict[str, Any] | None:
    for row in store.rows(RUN_LOG_MODULE):
        if int(row.get("id", 0)) == run_id:
            return row
    return None


def _used_run_ids() -> list[int]:
    """内存与运行日志中已经出现的编号，避免与历史记录撞号。"""
    ids = [int(row.get("id", 0)) for row in store.rows(RUN_LOG_MODULE)]
    for line in persist.read_jsonl("runs", RUN_LOG_FILE):
        if line.get("module") != RUN_LOG_MODULE:
            continue
        try:
            ids.append(int(line.get("entry_id")))
        except (TypeError, ValueError):
            continue
    return ids


def _next_run_id() -> int:
    return max(_used_run_ids(), default=0) + 1


def _log_record(
    spec: ReceiptSpec, entry: dict[str, Any], row: dict[str, Any]
) -> dict[str, Any]:
    """运行日志条目与内存记录同源，动作和来源标记固定由回执规则给出。"""
    return {
        "module": RUN_LOG_MODULE,
        "entity": RUN_LOG_ENTITY,
        "entry_id": row.get("id"),
        "status": row.get("status"),
        "version": row.get("version", ""),
        "action": spec.action,
        "timestamp": row.get("updated_at") or _now(),
        "abnormal": row.get("abnormal", False),
        "source_module": spec.source_module,
        "source_id": entry.get("id"),
    }


def _logged_receipt(
    spec: ReceiptSpec, entry: dict[str, Any], run_id: int | None = None
) -> dict[str, Any] | None:
    """查找运行日志中同一来源、同一动作的回执；给出 run_id 时要求编号一致。"""
    for line in reversed(persist.read_jsonl("runs", RUN_LOG_FILE)):
        if line.get("module") != RUN_LOG_MODULE:
            continue
        if line.get("action") != spec.action:
            continue
        if str(line.get("source_module")) != spec.source_module:
            continue
        if str(line.get("source_id")) != str(entry.get("id")):
            continue
        if run_id is not None:
            try:
                if int(line.get("entry_id")) != run_id:
                    continue
            except (TypeError, ValueError):
                continue
        return line
    return None


def _remove_memory_row(row: dict[str, Any]) -> None:
    rows = store.rows(RUN_LOG_MODULE)
    for index, candidate in enumerate(rows):
        if candidate is row:
            del rows[index]
            return


def _adopt_logged_receipt(
    spec: ReceiptSpec,
    entry: dict[str, Any],
    key: tuple[str, int],
    logged: dict[str, Any],
) -> dict[str, Any] | None:
    """日志已经落盘时，补齐对应内存记录并沿用原编号，不重复写日志。"""
    try:
        run_id = int(logged.get("entry_id"))
    except (TypeError, ValueError):
        return None
    row = _find_memory_row(run_id)
    if row is not None and not str(row.get("记录编号", "")).startswith(f"{spec.prefix}-"):
        # 编号被其他记录占用，不能覆盖，只能重新走一次登记。
        return None
    if row is None:
        row = spec.build_record(entry, run_id, logged.get("timestamp") or _now())
        store.rows(RUN_LOG_MODULE).append(row)
    _PENDING.pop(key, None)
    _FAILURES.pop(key, None)
    _COMMITTED[key] = row
    return row


def register_collection_receipt(
    spec: ReceiptSpec, entry: dict[str, Any]
) -> dict[str, Any] | None:
    """按来源登记一份采集运行回执，成功提交时返回内存记录。

    同一来源只保留一份回执：重试时先核对运行日志，已经提交的回执直接沿用；
    日志追加失败会回滚内存半成品并返回 None，同一入口可继续重试。
    """
    key = _receipt_key(spec, entry)
    with _LOCK:
        committed = _COMMITTED.get(key)
        if committed is not None and _find_memory_row(int(committed.get("id", 0))) is not None:
            return committed

        # 上一次登记在日志追加阶段中断：先核对日志，避免恢复时写出第二份记录。
        pending = _PENDING.get(key)
        logged = _logged_receipt(spec, entry, int(pending.get("id", 0)) if pending else None)
        if logged is None:
            logged = _logged_receipt(spec, entry)
        if logged is not None:
            adopted = _adopt_logged_receipt(spec, entry, key, logged)
            if adopted is not None:
                return adopted

        row = pending
        if row is None or _find_memory_row(int(row.get("id", 0))) is None:
            run_id = _next_run_id()
            row = spec.build_record(entry, run_id, _now())
            # 先建立内存记录，再尝试追加运行日志；失败会回滚这条记录。
            store.rows(RUN_LOG_MODULE).append(row)
            _PENDING[key] = row

        try:
            # 上一次中断可能留下没有换行的半行，先清理再接在完整记录后面。
            persist.repair_jsonl_tail("runs", RUN_LOG_FILE)
            persist.append_jsonl("runs", RUN_LOG_FILE, _log_record(spec, entry, row))
        except Exception as exc:  # 日志不可写：回滚半成品，编号不算正式占用
            _PENDING.pop(key, None)
            _remove_memory_row(row)
            _FAILURES[key] = {
                "source_module": spec.source_module,
                "source_id": entry.get("id"),
                "trigger": spec.trigger,
                "run_id": row.get("id"),
                "action": spec.action,
                "error": f"{type(exc).__name__}: {exc}",
            }
            return None

        _PENDING.pop(key, None)
        _FAILURES.pop(key, None)
        _COMMITTED[key] = row
        return row


def collection_receipt_state() -> dict[str, Any]:
    """按来源汇总回执提交结果：已提交、待提交和最近失败。"""
    with _LOCK:
        committed = [
            {
                "source_module": key[0],
                "source_id": key[1],
                "run_id": row.get("id"),
                "记录编号": row.get("记录编号"),
                "status": row.get("status"),
                "action": next(
                    (spec.action for spec in RECEIPT_SPECS if spec.source_module == key[0]), ""
                ),
                "trigger": next(
                    (spec.trigger for spec in RECEIPT_SPECS if spec.source_module == key[0]), ""
                ),
            }
            for key, row in _COMMITTED.items()
        ]
        pending = [
            {
                "source_module": key[0],
                "source_id": key[1],
                "run_id": row.get("id"),
                "记录编号": row.get("记录编号"),
                "status": row.get("status"),
            }
            for key, row in _PENDING.items()
        ]
        return {
            "committed": committed,
            "pending": pending,
            "failed": [dict(value) for value in _FAILURES.values()],
            "committed_count": len(committed),
            "pending_count": len(pending),
            "failed_count": len(_FAILURES),
        }


def reconcile_collection_receipts() -> dict[str, Any]:
    """按来源核对内存运行回执与运行日志，保证已提交回执一一对应。"""
    with _LOCK:
        memory: dict[int, dict[str, Any]] = {}
        for row in store.rows(RUN_LOG_MODULE):
            code = str(row.get("记录编号", ""))
            spec = next(
                (item for item in RECEIPT_SPECS if code.startswith(f"{item.prefix}-")), None
            )
            if spec is not None:
                memory[int(row.get("id", 0))] = {"spec": spec, "status": row.get("status")}

        log: dict[int, dict[str, Any]] = {}
        for line in persist.read_jsonl("runs", RUN_LOG_FILE):
            if line.get("module") != RUN_LOG_MODULE:
                continue
            spec = next(
                (item for item in RECEIPT_SPECS if item.action == line.get("action")), None
            )
            if spec is not None:
                log[int(line.get("entry_id", 0))] = {"spec": spec, "status": line.get("status")}

        matched = [
            {
                "run_id": run_id,
                "source_module": memory[run_id]["spec"].source_module,
                "trigger": memory[run_id]["spec"].trigger,
                "action": memory[run_id]["spec"].action,
                "memory_status": memory[run_id]["status"],
                "log_status": log[run_id]["status"],
            }
            for run_id in sorted(set(memory) & set(log))
        ]
        by_source: dict[str, int] = {}
        for item in matched:
            by_source[item["source_module"]] = by_source.get(item["source_module"], 0) + 1
        return {
            "matched": matched,
            "matched_count": len(matched),
            "memory_only": sorted(set(memory) - set(log)),
            "log_only": sorted(set(log) - set(memory)),
            "by_source": by_source,
        }
