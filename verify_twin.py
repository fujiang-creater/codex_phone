"""数字孪生（服务端侧）端到端验证。

自带一块"按固件语义应答"的模拟板：
  - 连 WS ?role=dog，先发 hello（故意不带 device_id，复刻板端现状）
  - servo.move / servo.unload / sequence.run / sequence.stop / action.* /
    motion.emergency_stop / servo.position.read / servo.battery.read / device.status
  - 复刻板端互斥：序列播放中拒绝 servo.move / sequence.run，读位置返回忙
  - 记录最近一次 servo.move 的完整 envelope，用来断言 mode/policy/args

跑法（服务需已起在 :8000/:8765）：
    python verify_twin.py
"""

import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.request

import websockets

BASE = "http://127.0.0.1:8000"
TOK = os.environ.get("DOG_TOKEN") or "fmj-zvndt-zslspc-vv"
WS = "ws://127.0.0.1:8765/?role=dog"

# ---------------- 模拟板状态 ----------------
POS = {}
SEQ = {"playing": False, "until": 0.0}
LAST_MOVE = {}
ENV_SEEN = []
T0 = time.time()


def _now():
    return time.time() - T0


def http(method, path, body=None, timeout=30):
    req = urllib.request.Request(
        BASE + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + TOK})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        try:
            return json.load(e)
        except Exception:  # noqa: BLE001
            return {"ok": False, "http_status": e.code, "raw": e.read().decode("utf-8", "replace")}


def result(rid, ok=True, status="succeeded", data=None, code=None, message=None, retryable=False):
    out = {"type": "command_result", "request_id": rid, "ok": ok}
    if ok:
        out["status"] = status
        if data:
            out["result"] = data
    else:
        out["error"] = {"code": code, "message": message, "retryable": retryable}
    return out


def seq_duration(steps):
    total = 0
    for st in steps or []:
        slowest = max([int(s.get("time") or 0) for s in (st.get("servos") or [])] or [0])
        total += max(slowest, int(st.get("delay_after") or 0))
    return total / 1000.0


def handle(cmd, args):
    """复刻 command_dispatcher.c 的行为。"""
    playing = SEQ["playing"] and time.time() < SEQ["until"]
    if not playing:
        SEQ["playing"] = False
    if cmd == "device.status":
        return True, {"state": "MOVING" if playing else "IDLE",
                      "servo_busy": playing, "sequence_playing": playing,
                      "free_psram": 3090000, "rssi": -52}
    if cmd in ("motion.emergency_stop", "action.stop", "sequence.stop"):
        SEQ["playing"] = False
        return True, {"status": "stopped"}
    if cmd == "servo.move":
        if playing:
            return False, ("INVALID_STATE", "service is not ready for this command", True)
        got = args.get("servos")
        if not isinstance(got, list) or not got:
            return False, ("INVALID_ARGUMENT", "servos required", False)
        global LAST_MOVE
        LAST_MOVE = {"servos": got, "at": _now()}
        for s in got:
            POS[int(s["id"])] = int(s["pos"])
        return True, {"status": "accepted", "count": len(got)}
    if cmd == "servo.unload":
        ids = args.get("ids") or []
        for i in ids:
            POS.pop(int(i), None)
        return True, {"status": "unloaded", "count": len(ids)}
    if cmd == "servo.position.read":
        if playing:
            return False, ("SERVICE_ERROR", "busy reading servo board", True)
        ids = args.get("ids") or []
        return True, {"positions": [{"id": int(i), "pos": int(POS.get(int(i), 500))} for i in ids]}
    if cmd == "servo.battery.read":
        if playing:
            return False, ("SERVICE_ERROR", "busy reading servo board", True)
        return True, {"voltage_mv": 7380}
    if cmd == "sequence.run":
        if playing:
            return False, ("INVALID_STATE", "sequence already playing", True)
        steps = args.get("steps")
        if not isinstance(steps, list) or not steps:
            return False, ("INVALID_ARGUMENT", "steps required", False)
        SEQ["playing"] = True
        SEQ["until"] = time.time() + seq_duration(steps)
        for st in reversed(steps):                      # 末端姿态近似
            for s in st.get("servos") or []:
                POS[int(s["id"])] = int(s["pos"])
            break
        return True, {"status": "accepted", "steps": len(steps)}
    if cmd == "action.run":
        if playing:
            return False, ("INVALID_STATE", "service is not ready for this command", True)
        if not isinstance(args.get("group"), int):
            return False, ("INVALID_ARGUMENT", "group required", False)
        SEQ["playing"] = True
        SEQ["until"] = time.time() + 1.2
        return True, {"status": "accepted"}
    if cmd == "action.speed":
        return True, {"status": "accepted"}
    if cmd == "display.face":
        return True, {"status": "accepted"}
    return False, ("NOT_IMPLEMENTED", "not implemented in mock", False)


async def mock_board(ready):
    async with websockets.connect(WS) as ws:
        await ws.send(json.dumps({
            "type": "hello", "version": 3, "transport": "websocket",
            "fw_version": "mock-1.0", "free_heap": 210000, "free_psram": 3090000,
            "rssi": -52, "uptime_sec": 120}))
        async for raw in ws:
            if isinstance(raw, (bytes, bytearray)):
                continue
            try:
                j = json.loads(raw)
            except Exception:  # noqa: BLE001
                continue
            if j.get("type") == "hello":
                ready.set()
                continue
            if j.get("type") != "command":
                continue
            ENV_SEEN.append(j)
            if len(ENV_SEEN) > 4000:
                del ENV_SEEN[:2000]
            ok, payload = handle(j.get("command"), j.get("args") or {})
            if ok:
                await ws.send(json.dumps(result(j.get("request_id"), True,
                                                data=payload), ensure_ascii=False))
            else:
                code, msg, retry = payload
                await ws.send(json.dumps(result(j.get("request_id"), False,
                                                code=code, message=msg,
                                                retryable=retry), ensure_ascii=False))


# ---------------- 断言 ----------------
PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ✓ " if cond else "  ✗ ") + name + (("  | " + str(detail)) if detail else ""))


def section(t):
    print("\n" + "=" * 74 + "\n" + t)


async def main():
    ready = asyncio.Event()
    board = asyncio.create_task(mock_board(ready))
    try:
        await asyncio.wait_for(ready.wait(), 15)
    except asyncio.TimeoutError:
        print("模拟板未能连上 WS（服务起了吗？检查 :8765）")
        board.cancel()
        return 2
    await asyncio.sleep(0.5)
    print("模拟板已连上 :8765（hello 不带 device_id，复刻板端现状）")

    async def call(method, path, body=None, timeout=30):
        return await asyncio.to_thread(http, method, path, body, timeout)

    async def wait_free(timeout=6.0):
        """等设备占用自然释放（瞬时命令按 hold_ms 过期，长命令靠板子上报）。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = await call("GET", "/api/v1/twin/status")
            if not r.get("target_busy"):
                return True
            await asyncio.sleep(0.2)
        return False

    section("1) 数据层：默认 store 与落盘")
    r = await call("GET", "/api/v1/twin/store")
    store = r.get("store") or {}
    check("GET /twin/store 返回默认值",
          len(store.get("calibration") or []) == 12
          and len(store.get("motion_groups") or []) == 10,
          f"标定 {len(store.get('calibration') or [])} 条 / 动作组 {len(store.get('motion_groups') or [])} 组")
    check("几何参数默认值正确", (store.get("geometry") or {}).get("thigh") == 60.0,
          store.get("geometry"))
    check("舵机 ID 默认 1..12", store.get("servo_ids") == list(range(1, 13)))

    r = await call("POST", "/api/v1/twin/store", {"patch": {
        "geometry": dict(store["geometry"], thigh=58.0),
        "calibration": [dict(store["calibration"][0], neutral=612)] + store["calibration"][1:],
        "motion_groups": [{"name": "握手测试", "steps": [
            {"servos": [{"id": 1, "pos": 300, "time": 500}], "delay_after": 400}]}]
        + store["motion_groups"][1:]}})
    check("POST /twin/store 写入成功", bool(r.get("ok")))
    r2 = await call("GET", "/api/v1/twin/store")
    s2 = r2.get("store") or {}
    check("回读：标定 neutral=612 已持久",
          (s2.get("calibration") or [{}])[0].get("neutral") == 612)
    check("回读：geometry.thigh=58 已持久",
          (s2.get("geometry") or {}).get("thigh") == 58.0)
    check("回读：动作组 1 步数 = 1",
          len((s2.get("motion_groups") or [{}])[0].get("steps") or []) == 1)
    disk = os.path.join(os.path.dirname(os.path.abspath(__file__)), "twin_store.json")
    check("twin_store.json 已落盘", os.path.isfile(disk), disk)
    if os.path.isfile(disk):
        with open(disk, encoding="utf-8") as f:
            on_disk = json.load(f)
        check("磁盘内容与接口一致（neutral=612）",
              on_disk["calibration"][0]["neutral"] == 612)

    section("2) 舵机代理：下发 / 读位置 / 电池")
    n0 = len(ENV_SEEN)
    r = await call("POST", "/api/v1/twin/servo/move",
                   {"servos": [{"id": 1, "pos": 520, "time": 300}]})
    check("POST /twin/servo/move 成功", bool(r.get("ok")), r.get("error"))
    env = ENV_SEEN[-1] if len(ENV_SEEN) > n0 else {}
    check("envelope: command=servo.move", env.get("command") == "servo.move")
    check("envelope: mode=sync（拿到真实回执）", env.get("mode") == "sync")
    check("envelope: policy=interrupt", env.get("policy") == "interrupt")
    check("envelope: args 透传 servos",
          (env.get("args") or {}).get("servos") == [{"id": 1, "pos": 520, "time": 300}])
    check("模拟板真收到了 pos=520", POS.get(1) == 520, POS)
    st = await call("GET", "/api/v1/twin/status")
    check("下发后立刻标记设备忙", bool(st.get("target_busy")), st.get("target_current"))
    check("瞬时命令按自身时长自动释放占用（不再死锁 30s）", await wait_free(3.0),
          "time=300 应在 ~0.6s 后释放")
    r = await call("GET", "/api/v1/twin/servo/pos?ids=1,2")
    pos1 = next((p["pos"] for p in (r.get("positions") or []) if p["id"] == 1), None)
    check("GET /twin/servo/pos 读回 pos=520", pos1 == 520, r.get("positions"))
    r = await call("GET", "/api/v1/twin/battery")
    check("GET /twin/battery 返回电压", r.get("voltage_mv") == 7380, r.get("voltage_mv"))

    section("3) 步态线程：启停、下发频率、占用互斥")
    await wait_free(3.0)
    r = await call("POST", "/api/v1/twin/gait/start", {})
    check("POST /twin/gait/start 成功", bool(r.get("ok")), r.get("error"))
    await asyncio.sleep(0.6)
    r0 = await call("GET", "/api/v1/twin/gait")
    g0 = r0.get("gait") or {}
    check("步态 running=true", bool(g0.get("running")))
    check("步态首拍有下发成功", (g0.get("sent") or 0) >= 1, f"sent={g0.get('sent')}")
    check("步态角度为 4 腿", len(g0.get("angles") or []) == 4, g0.get("angles"))
    n_move = len([e for e in ENV_SEEN if e.get("command") == "servo.move"])
    await asyncio.sleep(2.0)
    r1 = await call("GET", "/api/v1/twin/gait")
    g1 = r1.get("gait") or {}
    n_move2 = len([e for e in ENV_SEEN if e.get("command") == "servo.move"])
    rate = (n_move2 - n_move) / 2.0
    check("下发频率接近 deployMs=120ms（约 8 帧/s）", 5.0 <= rate <= 11.0, f"{rate:.1f} 帧/s")
    check("步态无下发失败", (g1.get("errors") or 0) == 0, g1.get("last_error"))
    snap1 = dict(POS)
    await asyncio.sleep(0.4)
    snap2 = dict(POS)
    check("舵机位置随步态变化（真的在驱动）", snap1 != snap2,
          f"{list(snap1.items())[:3]} -> {list(snap2.items())[:3]}")

    r = await call("POST", "/api/v1/twin/servo/move",
                   {"servos": [{"id": 3, "pos": 700, "time": 200}]})
    check("步态运行中：页面手动下发被门禁拒", not r.get("ok"),
          f"{r.get('error')} / {r.get('hint')}")
    r = await call("POST", "/api/v1/twin/sequence",
                   {"steps": [{"servos": [{"id": 1, "pos": 400, "time": 300}],
                               "delay_after": 300}]})
    check("步态运行中：动作组被拒", not r.get("ok"), r.get("error"))

    r = await call("POST", "/api/v1/twin/gait/stop", {})
    check("POST /twin/gait/stop 成功", bool(r.get("ok")))
    await asyncio.sleep(0.4)
    r = await call("GET", "/api/v1/twin/gait")
    check("停止后 running=false", not (r.get("gait") or {}).get("running"))
    r = await call("GET", "/api/v1/twin/status")
    check("停止后设备占用被释放", not r.get("target_busy"), r.get("target_current"))
    r = await call("POST", "/api/v1/twin/servo/move",
                   {"servos": [{"id": 3, "pos": 700, "time": 200}]})
    check("停止后可正常下发", bool(r.get("ok")), r.get("error"))

    section("4) 动作组：单次与循环")
    await wait_free(3.0)
    steps = [{"servos": [{"id": 1, "pos": 300, "time": 300}], "delay_after": 300},
             {"servos": [{"id": 1, "pos": 600, "time": 300}], "delay_after": 300}]
    r = await call("POST", "/api/v1/twin/sequence", {"steps": steps})
    check("单次 sequence.run 成功", bool(r.get("ok")), r.get("error"))
    r = await call("POST", "/api/v1/twin/sequence", {"steps": steps})
    check("序列播放中：再次 sequence.run 被板端拒（如实上报）", not r.get("ok"),
          r.get("error"))
    r = await call("POST", "/api/v1/twin/sequence/stop", {})
    check("sequence.stop 成功", bool(r.get("ok")))

    r = await call("POST", "/api/v1/twin/sequence",
                   {"steps": steps, "repeat": 3, "gap_ms": 60})
    check("循环播放已交给服务端线程", bool(r.get("ok")), r.get("error"))
    r = await call("GET", "/api/v1/twin/gait")
    sq = r.get("sequence") or {}
    check("循环 running=true", bool(sq.get("running")))
    deadline = time.time() + 20
    while time.time() < deadline:
        await asyncio.sleep(0.5)
        r = await call("GET", "/api/v1/twin/gait")
        sq = r.get("sequence") or {}
        if not sq.get("running"):
            break
    check("循环跑满 3 轮后自行停止", (sq.get("rounds") or 0) == 3 and not sq.get("running"),
          f"rounds={sq.get('rounds')} running={sq.get('running')} err={sq.get('last_error')}")
    r = await call("GET", "/api/v1/twin/status")
    check("循环结束后占用释放", not r.get("target_busy"), r.get("target_current"))

    section("5) 急停")
    await wait_free(5.0)
    await call("POST", "/api/v1/twin/gait/start", {})
    await asyncio.sleep(0.8)
    r = await call("POST", "/api/v1/twin/estop", {})
    check("estop 下发成功", bool(r.get("ok")), r.get("error"))
    check("estop envelope= motion.emergency_stop",
          ENV_SEEN[-1].get("command") == "motion.emergency_stop", ENV_SEEN[-1].get("command"))
    await asyncio.sleep(0.4)
    r = await call("GET", "/api/v1/twin/gait")
    check("estop 后步态不再运行", not (r.get("gait") or {}).get("running"))

    section("6) 恢复默认（把验证改过的数据还原）")
    r = await call("POST", "/api/v1/twin/store/reset", {"section": "calibration"})
    check("标定重置成功", bool(r.get("ok")))
    check("标定 neutral 回到 500",
          (r.get("store") or {}).get("calibration", [{}])[0].get("neutral") == 500)
    r = await call("POST", "/api/v1/twin/store/reset", {"section": "geometry"})
    check("几何重置成功（thigh 回到 60）",
          (r.get("store") or {}).get("geometry", {}).get("thigh") == 60.0)
    await call("POST", "/api/v1/twin/store",
               {"patch": {"motion_groups": [
                   {"name": f"动作组 {i+1}", "steps": []} for i in range(10)]}})
    r = await call("GET", "/api/v1/twin/store")
    check("动作组已清回空表",
          all(not g.get("steps") for g in (r.get("store") or {}).get("motion_groups") or []))

    board.cancel()
    print("\n" + "=" * 74)
    print(f"通过 {len(PASS)} 项，失败 {len(FAIL)} 项")
    for f in FAIL:
        print("  ✗ " + f)
    return 0 if not FAIL else 1


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
