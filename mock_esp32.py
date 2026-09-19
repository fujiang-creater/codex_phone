"""模拟 ESP32：连 WS -> hello -> 收命令回执 -> 收下行 Opus 帧，验证服务端调度中枢。"""
import asyncio, json, urllib.request, urllib.error
import websockets

BASE = "http://127.0.0.1:8000"
TOKEN = "dev-token"
DID = "dog-001"
EV = {"frames": 0, "cmds": [], "session_id": None}


def http(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={
        "Content-Type": "application/json", "Authorization": "Bearer " + TOKEN})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return {"http_error": e.code, "body": e.read().decode()[:200]}
    except Exception as e:
        return {"error": str(e)}


async def receiver(ws):
    while True:
        msg = await ws.recv()
        if isinstance(msg, (bytes, bytearray)):
            EV["frames"] += 1
            if EV["frames"] <= 2:
                print(f"   [bin] {len(msg)}B head={msg[:4].hex()}")
            continue
        j = json.loads(msg)
        t = j.get("type")
        if t == "command":
            EV["cmds"].append(j.get("command"))
            print(f"   [cmd] {j.get('command')} policy={j.get('policy')} "
                  f"mode={j.get('mode')} args={j.get('args')}")
            await ws.send(json.dumps({"type": "command_result",
                                      "request_id": j.get("request_id"),
                                      "command": j.get("command"),
                                      "ok": True, "error": None, "data": {}}))
        elif t == "audio_play":
            k = "play_" + str(j.get("state"))
            EV[k] = EV.get(k, 0) + 1
            print(f"   [audio_play] {j.get('state')} {j.get('audio_params') or ''}")


async def main():
    async with websockets.connect(f"ws://127.0.0.1:8765/?role=dog") as ws:
        await ws.send(json.dumps({
            "type": "hello", "version": 3, "transport": "websocket",
            "device_id": DID,
            "audio_params": {"format": "opus", "sample_rate": 16000,
                             "channels": 1, "frame_duration": 60}}))
        j = json.loads(await asyncio.wait_for(ws.recv(), 5))
        EV["session_id"] = j.get("session_id")
        print(f"1) hello 回包 session_id={EV['session_id']}")
        await ws.send(json.dumps({"type": "state", "device_id": DID, "state": "IDLE"}))
        await asyncio.sleep(0.3)

        rt = asyncio.create_task(receiver(ws))
        loop = asyncio.get_running_loop()
        call = lambda m, p, b=None: loop.run_in_executor(None, http, m, p, b)

        print("2) 舵机 servo.move (sync)")
        print("   ->", await call("POST", f"/api/v1/devices/{DID}/command",
                                  {"command": "servo.move",
                                   "args": {"servos": [{"id": 1, "pos": 1500, "time": 300}]},
                                   "mode": "sync"}))
        await asyncio.sleep(0.3)

        print("3) 表情 display.face happy")
        print("   ->", await call("POST", f"/api/v1/devices/{DID}/face", {"name": "happy"}))

        print("4) 错的表情名应被拒")
        print("   ->", await call("POST", f"/api/v1/devices/{DID}/face", {"name": "excited"}))

        print("5) 状态查询")
        r = await call("GET", "/api/v1/devices/state")
        print("   ->", json.dumps((r.get("devices") or {}).get(DID, {}), ensure_ascii=False))

        print("6) 下行播放（0.6s 提示音，60ms 一帧，应约 10 帧）")
        print("   ->", await call("POST", f"/api/v1/devices/{DID}/play", {"text": "你好"}))
        await asyncio.sleep(2.0)
        print(f"   收到 Opus 帧={EV['frames']} start={EV.get('play_start')} stop={EV.get('play_stop')}")

        print("7) 录音互斥：置 LISTENING 后请求播放应被拒")
        await ws.send(json.dumps({"type": "state", "device_id": DID, "state": "LISTENING"}))
        await asyncio.sleep(0.2)
        print("   ->", await call("POST", f"/api/v1/devices/{DID}/play", {"text": "应被拒"}))
        await ws.send(json.dumps({"type": "state", "device_id": DID, "state": "IDLE"}))
        await asyncio.sleep(0.2)

        print("8) 强制打断")
        await call("POST", f"/api/v1/devices/{DID}/play", {"text": "第一句"})
        await asyncio.sleep(0.2)
        print("   ->", await call("POST", f"/api/v1/devices/{DID}/audio/abort", {}))

        print("9) OTA（板端预留，应能下发）")
        print("   ->", await call("POST", f"/api/v1/devices/{DID}/ota/start", {}))
        await asyncio.sleep(0.3)
        print("10) OTA 门禁：期间舵机应被拒")
        print("   ->", await call("POST", f"/api/v1/devices/{DID}/command",
                                  {"command": "servo.move", "args": {}}))

        await asyncio.sleep(0.5)
        rt.cancel()
        print("\n汇总：收到命令", EV["cmds"], "| Opus帧", EV["frames"])

asyncio.run(main())
