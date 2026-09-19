"""对照板端协议文档，逐个验证服务端下发的 JSON 外壳与 mode/policy。
用法：python _verify_cmds.py
"""
import asyncio
import json
import threading
import time
import urllib.request

import websockets

BASE = "http://127.0.0.1:8000"
WS = "ws://127.0.0.1:8765/?role=dog"
DID = "dog-001"

# (命令, args, 期望 mode, 期望 policy)
CASES = [
    ("device.status", {}, "sync", "queue"),
    ("device.reboot", {}, "async", "interrupt"),
    ("display.face", {"name": "happy", "duration_ms": 5000}, "async", "queue"),
    ("servo.move", {"servos": [{"id": 1, "pos": 500, "time": 600}]}, "sync", "interrupt"),
    ("servo.unload", {"ids": [1, 2, 3]}, "sync", "interrupt"),
    ("servo.position.read", {"ids": [1, 2, 3]}, "sync", "queue"),
    ("servo.battery.read", {}, "sync", "queue"),
    ("action.run", {"group": 1, "times": 1}, "async", "queue"),
    ("action.stop", {}, "sync", "interrupt"),
    ("action.speed", {"group": 1, "percent": 100}, "sync", "queue"),
    ("sequence.run",
     {"steps": [{"servos": [{"id": 1, "pos": 300, "time": 500}], "delay_after": 800}]},
     "async", "queue"),
    ("sequence.stop", {}, "sync", "interrupt"),
    ("motion.emergency_stop", {}, "sync", "interrupt"),
    ("audio.capture.start", {}, "sync", "interrupt"),
    ("audio.capture.stop", {}, "sync", "interrupt"),
    ("audio.play.stop", {}, "sync", "interrupt"),
]

seen = []          # 板端收到的原始信封
ready = threading.Event()


async def board():
    async with websockets.connect(WS) as ws:
        await ws.send(json.dumps({
            "type": "hello", "version": 3, "transport": "websocket",
            "device_id": DID,
            "audio_params": {"format": "opus", "sample_rate": 16000,
                             "channels": 1, "frame_duration": 60}}))
        await ws.recv()          # hello 回包
        ready.set()
        while True:
            msg = await ws.recv()
            if isinstance(msg, bytes):
                continue
            j = json.loads(msg)
            if j.get("type") == "command":
                seen.append(j)
                await ws.send(json.dumps({
                    "type": "command_result", "request_id": j.get("request_id"),
                    "command": j.get("command"), "ok": True, "error": None, "data": {}}))
                # 模拟真实板子：动作执行完回报空闲，让服务端释放占用
                if not (j.get("command") or "").startswith("device."):
                    await ws.send(json.dumps({"type": "state", "device_id": DID,
                                              "state": "IDLE"}))


def http_post(path, body):
    req = urllib.request.Request(
        BASE + path, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer fmj-zvndt-zslspc-vv"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


async def main():
    t = asyncio.create_task(board())
    await asyncio.sleep(0.5)
    ready.wait(10)
    ok_cnt = 0
    for cmd, args, exp_mode, exp_pol in CASES:
        seen.clear()
        st, res = http_post(f"/api/v1/devices/{DID}/command", {"command": cmd, "args": args})
        await asyncio.sleep(0.35)
        env = seen[0] if seen else None
        if env is None:
            print(f"  [FAIL] {cmd:<24} 板端未收到命令 http={st} res={res}")
            continue
        # 逐字段校验外壳
        errs = []
        if env.get("type") != "command":
            errs.append(f"type={env.get('type')}")
        if env.get("version") != 1:
            errs.append(f"version={env.get('version')}")
        if not env.get("request_id"):
            errs.append("request_id 缺失")
        if env.get("command") != cmd:
            errs.append(f"command={env.get('command')}")
        if env.get("args") != args:
            errs.append(f"args={env.get('args')}")
        if env.get("mode") != exp_mode:
            errs.append(f"mode={env.get('mode')} 期望 {exp_mode}")
        if env.get("policy") != exp_pol:
            errs.append(f"policy={env.get('policy')} 期望 {exp_pol}")
        prio = res.get("priority")
        if errs:
            print(f"  [FAIL] {cmd:<24} " + "; ".join(errs))
        else:
            ok_cnt += 1
            print(f"  [OK]   {cmd:<24} P{prio} mode={env['mode']:<6} policy={env['policy']:<9} "
                  f"rid={env['request_id'][-6:]}")
    print(f"\n合计：{ok_cnt}/{len(CASES)} 条命令的外壳与 mode/policy 完全符合协议")
    t.cancel()


asyncio.run(main())
