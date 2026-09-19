"""临时验证脚本：用"假的真板子"跑一遍服务端 ↔ 板端适配。

模拟板完全按 esp32s3 固件的行为应答：
- hello 不带 device_id（板端现状），带 fw_version/free_heap/free_psram/rssi
- state 上报不带 state 字段，只有 servo_busy 等布尔量
- command_result 按 command_dispatcher.c 的语义回 ok/error
- action.run group=12 故意回 409 INVALID_STATE，用来验证失败补话

跑完打印服务端实际下发的 envelope，核对 args/policy/mode。
"""

import asyncio
import json
import sys
import time
import urllib.error
import urllib.request

import websockets

BASE = "http://127.0.0.1:8000"
TOK = "fmj-zvndt-zslspc-vv"
RECEIVED = []


def http(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer " + TOK})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:      # 409/503 也是有效结果，把响应体读出来
        try:
            return json.load(e)
        except Exception:  # noqa: BLE001
            return {"http_error": e.code}


async def mock_board(ready: asyncio.Event):
    async with websockets.connect("ws://127.0.0.1:8765/?role=xiaozhi") as ws:
        # 板端现状：hello 里没有 device_id
        await ws.send(json.dumps({
            "type": "hello", "version": 3, "transport": "websocket",
            "audio_params": {"format": "opus", "sample_rate": 16000,
                             "channels": 1, "frame_duration": 60},
            "fw_version": "1.4.2-board", "free_heap": 178000,
            "free_psram": 3100000, "rssi": -52,
        }, ensure_ascii=False))
        await ws.recv()
        ready.set()
        # 板端现状：state 上报没有 state 字段，只有布尔量 + 原始字段名 free_psram
        await ws.send(json.dumps({"type": "state", "servo_busy": True,
                                  "free_psram": 3090000, "free_heap": 176000,
                                  "rssi": -54}, ensure_ascii=False))
        while True:
            msg = await ws.recv()
            if isinstance(msg, (bytes, bytearray)):
                continue
            j = json.loads(msg)
            if j.get("type") != "command":
                continue
            RECEIVED.append(j)
            cmd = j.get("command")
            rid = j.get("request_id")
            args = j.get("args") or {}
            if cmd == "action.run" and args.get("group") == 12:
                res = {"type": "command_result", "request_id": rid, "ok": False,
                       "error": {"code": "INVALID_STATE",
                                 "message": "service is not ready for this command",
                                 "retryable": True}}
            elif cmd == "action.run":
                res = {"type": "command_result", "request_id": rid, "ok": True,
                       "status": "accepted", "result": {"group": args.get("group")}}
            elif cmd == "display.face":
                res = {"type": "command_result", "request_id": rid, "ok": True,
                       "status": "succeeded", "result": {"name": args.get("name")}}
            else:
                res = {"type": "command_result", "request_id": rid, "ok": True,
                       "status": "succeeded", "result": {}}
            await ws.send(json.dumps(res, ensure_ascii=False))


async def main():
    ready = asyncio.Event()
    task = asyncio.create_task(mock_board(ready))
    await asyncio.wait_for(ready.wait(), timeout=30)
    await asyncio.sleep(1.5)

    # HTTP 一律扔线程：同步调用会堵住本进程事件循环，mock 板就收不到命令了
    async def call(method, path, body=None):
        return await asyncio.to_thread(http, method, path, body)

    print("=" * 72)
    print("1) 设备表字段归一（板端 free_psram -> UI psram_free）")
    devs = await call("GET", "/api/v1/devices")
    rows = devs.get("devices") or devs.get("items") or []
    for d in rows:
        print("   ", json.dumps({k: d.get(k) for k in
              ("device_id", "state", "online", "fw_version", "rssi",
               "free_heap", "psram_free", "servo_busy")}, ensure_ascii=False))

    print("=" * 72)
    print("2) 大脑层预演（只问模型，不下发）")
    for t in ("坐下", "给我叫一声", "握个手"):
        r = await call("POST", "/api/v1/brain/test", {"device_id": "verify", "text": t})
        dbg = r.get("debug") or {}
        print(f"    {t:<8} -> 说:{r.get('reply')!r} 动作={r.get('actions')} "
              f"表情={r.get('face')} {dbg.get('latency_ms')}ms")

    print("=" * 72)
    print("3) 真下发：sync + args.group（应成功，动作进 executed.actions）")
    # 模型偶尔这轮只说话不调动作（采样随机），所以重试几次直到它真的挑了一个动作
    for attempt in range(5):
        r = await call("POST", "/api/v1/brain/say",
                       {"device_id": "127.0.0.1", "text": "坐下", "wait": True})
        ex = r.get("executed") or {}
        if (r.get("actions") or []):
            print(f"    第{attempt+1}次 reply={r.get('reply')!r} "
                  f"执行={json.dumps(ex, ensure_ascii=False)}")
            break
        print(f"    第{attempt+1}次 模型只说话没选动作，重试…")

    print("=" * 72)
    print("4) 板端拒绝（409）能否被服务端如实抬出来")
    r = await call("POST", "/api/v1/devices/127.0.0.1/command",
                   {"command": "action.run", "args": {"group": 12, "times": 1},
                    "policy": "interrupt", "mode": "sync"})
    print("   ", json.dumps({k: r.get(k) for k in
                             ("ok", "error", "error_code", "retryable")},
                            ensure_ascii=False))

    print("=" * 72)
    print("4b) 大脑层走同一条路：动作被拒 -> 触发补话")
    for attempt in range(5):
        r = await call("POST", "/api/v1/brain/say",
                       {"device_id": "127.0.0.1", "text": "叫一声", "wait": True})
        if (r.get("actions") or []):
            print(f"    第{attempt+1}次 reply={r.get('reply')!r} "
                  f"执行={json.dumps(r.get('executed'), ensure_ascii=False)}")
            print("    notes:", json.dumps((r.get("debug") or {}).get("notes"),
                                           ensure_ascii=False))
            break
        print(f"    第{attempt+1}次 模型只说话没选动作，重试…")

    await asyncio.sleep(0.5)
    print("=" * 72)
    print("5) 服务端实际下发的 envelope（核对 args/policy/mode）")
    for j in RECEIVED:
        print("   ", json.dumps({k: j.get(k) for k in
              ("command", "mode", "policy", "args")}, ensure_ascii=False))

    task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:  # noqa: BLE001
        print("验证失败：", e)
        sys.exit(1)
