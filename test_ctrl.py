#!/usr/bin/env python3
"""设备控制/OTA 服务端全流程回归测试（HTTP 局域网轮询协议 v1）。

用法：先把 servev1.py 用 --ctrl-only 跑起来（默认 :8000），再运行本脚本：
    python test_ctrl.py
可选环境变量 CTRL_BASE(默认 http://127.0.0.1:8000) / CTRL_TOKEN(默认 esp32s3-device-token)。

覆盖：health / 心跳 / 固件 latest / 下发 OTA / 轮询 next / OTA 状态机 /
固件下载 / 单指令互斥(409) / 鉴权(401) / 未知指令类型(400) / 终态后 next=204。
"""
import json
import os
import sys
import urllib.request
import urllib.error

BASE = os.environ.get("CTRL_BASE", "http://127.0.0.1:8000")
TOKEN = os.environ.get("CTRL_TOKEN", "esp32s3-device-token")
DID = "esp32s3-001"

PASS = 0
FAIL = 0


def _req(method, path, body=None, token=TOKEN, raw=False):
    url = BASE + path
    data = None
    headers = {"Authorization": "Bearer " + token}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            code = r.getcode()
            payload = r.read()
            if raw:
                return code, payload, dict(r.headers)
            try:
                return code, json.loads(payload.decode("utf-8")), dict(r.headers)
            except Exception:
                return code, payload.decode("utf-8", "replace"), dict(r.headers)
    except urllib.error.HTTPError as e:
        payload = e.read()
        try:
            j = json.loads(payload.decode("utf-8"))
        except Exception:
            j = payload.decode("utf-8", "replace")
        return e.code, j, dict(e.headers)


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name}  {detail}")


def main():
    print(f"== 控制服务测试 @ {BASE} ==")
    # 1. health
    code, j, _ = _req("GET", "/health")
    check("GET /health -> 200 ok", code == 200 and j.get("ok") is True, f"{code}/{j}")

    # 2. 鉴权：无 token -> 401
    code, _, _ = _req("GET", "/api/v1/devices", token="")
    check("GET /api/v1/devices 无 token -> 401", code == 401, f"code={code}")

    # 3. 鉴权：错误 token -> 401
    code, _, _ = _req("GET", "/api/v1/devices", token="wrong-token")
    check("GET /api/v1/devices 错误 token -> 401", code == 401, f"code={code}")

    # 4. 心跳登记
    hb = {"device_id": DID, "fw_version": "1.0.0", "running_slot": "ota_0",
          "ip": "192.168.1.120", "rssi": -55, "uptime_sec": 3600,
          "free_heap": 180000, "psram_free": 7000000, "busy": False}
    code, j, _ = _req("POST", f"/api/v1/devices/{DID}/heartbeat", hb)
    check("POST heartbeat -> 200 ok+server_time", code == 200 and "server_time" in j, f"{code}/{j}")

    # 5. 设备列表可见
    code, j, _ = _req("GET", "/api/v1/devices")
    devs = j.get("devices", [])
    check("GET /api/v1/devices 含心跳设备", code == 200 and any(d["device_id"] == DID for d in devs), f"{code}/{j}")

    # 6. 固件 latest
    code, j, _ = _req("GET", "/api/v1/firmware/esp32s3/latest")
    check("GET firmware latest -> 含 version/size/sha256/url",
          code == 200 and all(k in j for k in ("version", "size", "sha256", "url")), f"{code}/{j}")

    # 7. 下发 OTA（服务端自动补全 url/size/sha256）
    code, j, _ = _req("POST", f"/api/v1/devices/{DID}/commands",
                       {"type": "ota", "params": {"version": "1.0.1"}})
    ota = j.get("command", {})
    check("POST ota 下发成功并补全参数",
          code == 200 and j.get("ok") and ota.get("type") == "ota"
          and "url" in ota["params"] and "sha256" in ota["params"], f"{code}/{j}")
    cid = ota.get("id")

    # 8. 轮询 next -> 200 返回该 OTA 指令（且返回后状态置 accepted，不再重发）
    code, j, _ = _req("GET", f"/api/v1/devices/{DID}/commands/next")
    check("GET commands/next -> 200 返回 OTA 指令", code == 200 and j.get("ok")
          and j["command"]["id"] == cid, f"{code}/{j}")
    code, j, _ = _req("GET", f"/api/v1/devices/{DID}/commands/next")
    check("再次 next -> 204（已 accepted 不重发）", code == 204, f"code={code}")

    # 9. OTA 状态机
    seq = [("downloading", 35), ("verifying", 70), ("rebooting", 90)]
    ok_seq = True
    for st, prog in seq:
        code, j, _ = _req("POST", f"/api/v1/devices/{DID}/commands/{cid}/status",
                          {"status": st, "progress": prog, "message": st})
        if code != 200:
            ok_seq = False
    check("OTA downloading/verifying/rebooting 状态上报均 200", ok_seq)

    # 10. 终态 succeeded -> 队列清空
    code, j, _ = _req("POST", f"/api/v1/devices/{DID}/commands/{cid}/status",
                      {"status": "succeeded", "progress": 100, "message": "ota completed"})
    check("OTA succeeded 上报 200", code == 200, f"code={code}")
    code, j, _ = _req("GET", f"/api/v1/devices/{DID}/commands/next")
    check("succeeded 后 next -> 204（队列清空）", code == 204, f"code={code}")

    # 11. 单指令互斥：下发 servo_action 后再下发 -> 409
    code, j, _ = _req("POST", f"/api/v1/devices/{DID}/commands",
                      {"type": "servo_action", "params": {"servo_id": 1, "action": "forward", "duration_ms": 1000}})
    check("下发 servo_action 成功", code == 200 and j.get("ok"), f"{code}/{j}")
    code, j, _ = _req("POST", f"/api/v1/devices/{DID}/commands",
                      {"type": "stop", "params": {}})
    check("设备忙时再下发 -> 409", code == 409, f"code={code} {j}")
    # 清理该指令（标记 failed 终态以清空队列）
    cid2 = j2 if False else None
    # 取出当前指令 id 并终态化
    _, cur, _ = _req("GET", f"/api/v1/devices/{DID}/commands/next")
    if cur.get("ok"):
        cur_cid = cur["command"]["id"]
        _req("POST", f"/api/v1/devices/{DID}/commands/{cur_cid}/status",
             {"status": "failed", "progress": 0, "error_code": "TEST_CLEANUP"})

    # 12. 未知指令类型 -> 400
    code, j, _ = _req("POST", f"/api/v1/devices/{DID}/commands",
                      {"type": "nuke_everything", "params": {}})
    check("未知指令类型 -> 400", code == 400, f"code={code}")

    # 13. 固件下载（鉴权 + Content-Length + 内容一致）
    code, raw, hdrs = _req("GET", "/firmware/esp32s3/1.0.1/app.bin", raw=True)
    exp = None
    _, lj, _ = _req("GET", "/api/v1/firmware/esp32s3/latest")
    exp_size = lj.get("size")
    exp_sha = lj.get("sha256")
    import hashlib
    got_sha = hashlib.sha256(raw).hexdigest() if raw else None
    check("固件下载 200 + Content-Length 匹配 + sha256 一致",
          code == 200 and str(hdrs.get("Content-Length")) == str(exp_size)
          and got_sha == exp_sha, f"code={code} len={hdrs.get('Content-Length')} exp={exp_size} sha={got_sha==exp_sha}")

    # 14. 固件下载无 token -> 401
    code, _, _ = _req("GET", "/firmware/esp32s3/1.0.1/app.bin", token="")
    check("固件下载无 token -> 401", code == 401, f"code={code}")

    print(f"\n== 结果：PASS {PASS} / FAIL {FAIL} ==")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
