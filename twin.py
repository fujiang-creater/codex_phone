"""数字孪生（服务端侧）：姿态数据落盘 + trot 步态发生器。

这部分原本整个活在板端内存里 —— main/web_page.h 内嵌了 31KB HTML，
FK/IK、步态、标定表、动作组全在浏览器 JS 里跑：

  - 标定表、动作组存在浏览器本地存储 -> 换机器/清缓存即丢，大模型也读不到
  - 步态跑在 requestAnimationFrame -> 标签页转后台被降到 1Hz、合盖即停，
    无法被语音/大模型/定时任务触发
  - 页面直连板端 /api/* -> 绕过了服务端的鉴权、优先级、互斥、去重、回执对账

现在搬到服务端：

  - 标定表 / 几何参数 / 动作组 -> twin_store.json（可备份、可版本管理、大模型可读）
  - trot 步态 -> 后台线程（与浏览器无关，页面关掉照样走）
  - 所有下发 -> 统一走 servev1 的 WS 命令层

运动学公式与标定公式与板端页面**逐行等价**，只是换了执行位置 —— 这样从页面
切过来的用户不会觉得姿态变了。改动运动学时请同步改板端，或干脆只改这里。
"""

import copy
import json
import math
import os
import threading
import time

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STORE_PATH = os.path.join(_SCRIPT_DIR, "twin_store.json")

DEG = math.pi / 180.0
SCALE_DEFAULT = 1000.0 / 270.0          # 与板端页面一致：1000 计数值 / 270 度

LEGS = (
    {"name": "FL", "fx": 1, "side": 1, "front": True},
    {"name": "FR", "fx": 1, "side": -1, "front": True},
    {"name": "BL", "fx": -1, "side": 1, "front": False},
    {"name": "BR", "fx": -1, "side": -1, "front": False},
)
DOF = ("a0", "a1", "a2")
DOF_LABEL = ("外展", "大腿", "小腿")

DEFAULT_GEOMETRY = {"bodyL": 255.3, "bodyW": 120, "hipX": 96, "hipZ": 96,
                    "thigh": 60, "shinF": 62.1, "shinB": 77}
DEFAULT_GAIT = {"period": 0.7, "stride": 70, "lift": 35, "bodyHeight": 95,
                "moveMs": 500, "deployMs": 120, "offset": [0, 0.5, 0.5, 0]}
DEFAULT_SERVO_IDS = list(range(1, 13))

MOTION_GROUP_COUNT = 10
MOTION_MAX_STEPS = 20
STEP_MAX_DELAY_MS = 60000

ANGLE_LIMITS = {"a0": (-60.0, 60.0), "a1": (-120.0, 120.0), "a2": (-150.0, 150.0)}

_lock = threading.RLock()
_cache = {"mtime": None, "store": None}


def clamp(v, lo, hi):
    return min(hi, max(lo, v))


def _num(v, default, lo=None, hi=None):
    try:
        x = float(v)
    except (TypeError, ValueError):
        x = float(default)
    if x != x or x in (float("inf"), float("-inf")):     # NaN / inf 兜底
        x = float(default)
    if lo is not None or hi is not None:
        x = clamp(x, lo if lo is not None else x, hi if hi is not None else x)
    return x


def _int(v, default, lo=None, hi=None):
    return int(round(_num(v, default, lo, hi)))


def default_calibration():
    """12 条：按腿序 FL/FR/BL/BR，每条腿 a0/a1/a2。索引 = 腿序*3 + 关节序。"""
    return [{"leg": leg["name"], "dof": DOF[i], "label": DOF_LABEL[i],
             "neutral": 500, "dir": 1, "scale": SCALE_DEFAULT,
             "min": 0, "max": 1000}
            for leg in LEGS for i in range(3)]


def default_store():
    return {
        "version": 1,
        "geometry": dict(DEFAULT_GEOMETRY),
        "servo_ids": list(DEFAULT_SERVO_IDS),
        "calibration": default_calibration(),
        "gait": copy.deepcopy(DEFAULT_GAIT),
        "motion_groups": [{"name": "动作组 %d" % (i + 1), "steps": []}
                          for i in range(MOTION_GROUP_COUNT)],
        "updated_at": None,
    }


# ---------------------------------------------------------------- 存储读写
def _sanitize(store):
    """把外部写进来的东西洗一遍：长度、类型、量程。宁可修正也不报错崩掉。"""
    out = default_store()
    if not isinstance(store, dict):
        return out

    geom = store.get("geometry")
    if isinstance(geom, dict):
        for k, dv in DEFAULT_GEOMETRY.items():
            out["geometry"][k] = _num(geom.get(k), dv, 1.0, 500.0)

    ids = store.get("servo_ids")
    if isinstance(ids, list):
        cleaned = [_int(v, 0, 1, 253) for v in ids[:12]]
        cleaned = [v for v in cleaned if v > 0]
        if cleaned:
            while len(cleaned) < 12:
                cleaned.append(cleaned[len(cleaned) % len(cleaned)])
            out["servo_ids"] = cleaned

    cal = store.get("calibration")
    if isinstance(cal, list) and len(cal) == 12:
        for i, item in enumerate(cal):
            if not isinstance(item, dict):
                continue
            base = out["calibration"][i]
            base["neutral"] = _int(item.get("neutral"), 500, 0, 1000)
            base["dir"] = 1 if _num(item.get("dir"), 1) >= 0 else -1
            base["scale"] = _num(item.get("scale"), SCALE_DEFAULT, 0.001, 100.0)
            base["min"] = _int(item.get("min"), 0, 0, 1000)
            base["max"] = _int(item.get("max"), 1000, 0, 1000)
            if base["min"] > base["max"]:
                base["min"], base["max"] = base["max"], base["min"]

    gait = store.get("gait")
    if isinstance(gait, dict):
        g = out["gait"]
        g["period"] = _num(gait.get("period"), 0.7, 0.05, 10.0)
        g["stride"] = _num(gait.get("stride"), 70, 0, 300)
        g["lift"] = _num(gait.get("lift"), 35, 0, 200)
        g["bodyHeight"] = _num(gait.get("bodyHeight"), 95, 10, 300)
        g["moveMs"] = _int(gait.get("moveMs"), 500, 0, 65535)
        g["deployMs"] = _int(gait.get("deployMs"), 120, 40, 2000)
        off = gait.get("offset")
        if isinstance(off, list) and len(off) == 4:
            g["offset"] = [round(_num(v, 0), 4) for v in off]

    groups = store.get("motion_groups")
    if isinstance(groups, list):
        out["motion_groups"] = [_sanitize_group(groups[i], i)
                                for i in range(min(MOTION_GROUP_COUNT, len(groups)))]
        while len(out["motion_groups"]) < MOTION_GROUP_COUNT:
            i = len(out["motion_groups"])
            out["motion_groups"].append({"name": "动作组 %d" % (i + 1), "steps": []})

    if store.get("updated_at"):
        out["updated_at"] = str(store["updated_at"])
    return out


def _sanitize_group(group, index):
    g = {"name": "动作组 %d" % (index + 1), "steps": []}
    if not isinstance(group, dict):
        return g
    if isinstance(group.get("name"), str) and group["name"].strip():
        g["name"] = group["name"].strip()[:40]
    steps = group.get("steps")
    if isinstance(steps, list):
        for st in steps[:MOTION_MAX_STEPS]:
            s = _sanitize_step(st)
            if s:
                g["steps"].append(s)
    return g


def _sanitize_step(step):
    if not isinstance(step, dict) or not isinstance(step.get("servos"), list):
        return None
    servos, seen = [], set()
    for raw in step["servos"]:
        if not isinstance(raw, dict):
            continue
        sid = _int(raw.get("id"), 0, 1, 253)
        if sid <= 0 or sid in seen:
            continue
        seen.add(sid)
        servos.append({"id": sid,
                       "pos": _int(raw.get("pos"), 500, 0, 1000),
                       "time": _int(raw.get("time"), 300, 0, 65535)})
    if not servos:
        return None
    return {"servos": servos,
            "delay_after": _int(step.get("delay_after"), 150, 0, STEP_MAX_DELAY_MS)}


def load():
    """读 store：按 mtime 缓存，文件一改下一拍自动生效（免重启）。"""
    with _lock:
        try:
            mt = os.path.getmtime(STORE_PATH)
        except OSError:
            mt = None
        if mt != _cache["mtime"] or _cache["store"] is None:
            raw = None
            if mt is not None:
                try:
                    with open(STORE_PATH, "r", encoding="utf-8") as f:
                        raw = json.load(f)
                except Exception:  # noqa: BLE001
                    print("[twin] twin_store.json 解析失败，暂用默认值", flush=True)
            _cache["store"] = _sanitize(raw) if raw is not None else default_store()
            _cache["mtime"] = mt
        return copy.deepcopy(_cache["store"])


def save(patch, persist=True):
    """把 patch 合进 store。patch 里给了哪个顶层键就换哪个（几何/标定/步态/动作组）。"""
    with _lock:
        cur = load()
        if not isinstance(patch, dict):
            raise ValueError("patch 必须是对象")
        for k in ("geometry", "servo_ids", "calibration", "gait", "motion_groups"):
            if k in patch:
                cur[k] = patch[k]
        cur = _sanitize(cur)
        cur["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        if persist:
            tmp = STORE_PATH + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(cur, f, ensure_ascii=False, indent=2)
            os.replace(tmp, STORE_PATH)
            try:
                _cache["mtime"] = os.path.getmtime(STORE_PATH)
            except OSError:
                _cache["mtime"] = None
            _cache["store"] = cur
        return copy.deepcopy(cur)


def reset_section(section):
    """把某一节恢复默认（标定跑飞了的时候很有用）。"""
    d = default_store()
    if section not in d:
        raise ValueError("unknown section: %s" % section)
    return save({section: d[section]})


# ---------------------------------------------------------------- 运动学
def ik_solve(geom, leg_index, x, y):
    """单腿逆解（与板端页面同式），返回角度制的 {a1, a2}。"""
    l1 = _num(geom.get("thigh"), 60, 1, 500)
    front = LEGS[leg_index]["front"]
    l2 = _num(geom.get("shinF") if front else geom.get("shinB"), 62.1, 1, 500)
    d = clamp(math.hypot(x, y), abs(l1 - l2) + 0.5, l1 + l2 - 0.5)
    phi = math.atan2(x, -y)
    alpha = math.acos(clamp((l1 * l1 + d * d - l2 * l2) / (2 * l1 * d), -1.0, 1.0))
    beta = math.acos(clamp((l1 * l1 + l2 * l2 - d * d) / (2 * l1 * l2), -1.0, 1.0))
    return {"a1": (phi + alpha) / DEG, "a2": -(math.pi - beta) / DEG}


def _clamp_angles(a1, a2):
    return [0.0,
            clamp(a1, *ANGLE_LIMITS["a1"]),
            clamp(a2, *ANGLE_LIMITS["a2"])]


def stand_angles(store):
    """站立姿态：四腿按站高反解，外展归零。"""
    geom = store.get("geometry") or DEFAULT_GEOMETRY
    h = _num((store.get("gait") or {}).get("bodyHeight"), 95, 10, 300)
    out = []
    for i in range(4):
        sol = ik_solve(geom, i, 0.0, -h)
        out.append(_clamp_angles(sol["a1"], sol["a2"]))
    return out


def angles_at(store, t):
    """trot 步态：给定相位时间 t（秒），算出四腿关节角（度）。"""
    g = store.get("gait") or DEFAULT_GAIT
    geom = store.get("geometry") or DEFAULT_GEOMETRY
    period = max(0.05, _num(g.get("period"), 0.7, 0.05, 10.0))
    stride = _num(g.get("stride"), 70, 0, 300)
    lift = _num(g.get("lift"), 35, 0, 200)
    height = _num(g.get("bodyHeight"), 95, 10, 300)
    offsets = g.get("offset") or [0, 0.5, 0.5, 0]
    out = []
    for i in range(4):
        ph = (t / period + _num(offsets[i] if i < len(offsets) else 0, 0)) % 1.0
        if ph < 0.5:                      # 支撑相：脚向后拖
            s = ph / 0.5
            x = stride * (0.5 - s)
            lz = 0.0
        else:                             # 摆动相：抬脚前摆
            s = (ph - 0.5) / 0.5
            x = stride * (-0.5 + s)
            lz = lift * math.sin(math.pi * s)
        sol = ik_solve(geom, i, x, -height + lz)
        out.append(_clamp_angles(sol["a1"], sol["a2"]))
    return out


def angles_to_servos(store, angles, move_ms=None):
    """关节角 -> 舵机报文。pos = neutral + dir*deg*scale，再夹到 min..max。"""
    cal = store.get("calibration") or default_calibration()
    ids = store.get("servo_ids") or DEFAULT_SERVO_IDS
    default_cal = default_calibration()
    ms = _int(move_ms if move_ms is not None
              else (store.get("gait") or {}).get("moveMs"), 500, 0, 65535)
    servos = []
    for li in range(4):
        for d in range(3):
            idx = li * 3 + d
            c = cal[idx] if idx < len(cal) else default_cal[idx]
            sid = _int(ids[idx] if idx < len(ids) else idx + 1, idx + 1, 1, 253)
            deg = angles[li][d]
            pos = int(round(clamp(
                _num(c.get("neutral"), 500) + _num(c.get("dir"), 1) * deg
                * _num(c.get("scale"), SCALE_DEFAULT),
                _num(c.get("min"), 0), _num(c.get("max"), 1000))))
            servos.append({"id": sid, "pos": pos, "time": ms})
    return servos


def sequence_duration_ms(steps):
    """估算一条 sequence.run 要跑多久（板端没有动作完成事件，只能算）。

    每一步的耗时 = max(该步里最慢的舵机 time, delay_after)。
    """
    total = 0
    for st in steps or []:
        servos = (st or {}).get("servos") or []
        slowest = max([_int(s.get("time"), 0, 0, 65535) for s in servos] or [0])
        total += max(slowest, _int((st or {}).get("delay_after"), 0, 0, STEP_MAX_DELAY_MS))
    return total


# ---------------------------------------------------------------- 步态线程
class GaitRunner(object):
    """trot 步态发生器：自己算 IK、自己按周期下发。

    每拍都重新读一次 store，所以在页面上拖滑块能立刻生效（store 按 mtime 缓存）。

    dispatch(device_id, servos, move_ms) 由 servev1 注入。它直接走 WS 下发，
    不经过 dev_gate —— 因为步态自己就是这台设备的当前占用者，否则会被自家的
    busy 门禁挡下。
    """

    MAX_CONSECUTIVE_FAIL = 5

    def __init__(self, store_get, dispatch, on_start=None, on_stop=None):
        self._store_get = store_get
        self._dispatch = dispatch
        self._on_start = on_start
        self._on_stop = on_stop
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self.device_id = None
        self.started_at = None
        self.finished_at = None
        self.ticks = 0
        self.sent = 0
        self.errors = 0
        self.last_error = None
        self.last_result = None
        self.angles = None
        self._elapsed = 0.0
        self._fail_streak = 0

    # ---- 查询 ----
    def is_running(self, device_id=None):
        with self._lock:
            alive = bool(self._thread and self._thread.is_alive())
            if not alive:
                return False
            return device_id is None or self.device_id == device_id

    def state(self):
        with self._lock:
            running = bool(self._thread and self._thread.is_alive())
            return {
                "running": running,
                "device_id": self.device_id,
                "elapsed_s": round(self._elapsed, 2),
                "ticks": self.ticks,
                "sent": self.sent,
                "errors": self.errors,
                "last_error": self.last_error,
                "angles": copy.deepcopy(self.angles),
                "store_updated_at": (self._store_get() or {}).get("updated_at"),
                "last_result": self.last_result,
            }

    # ---- 启停 ----
    def start(self, device_id, overrides=None):
        with self._lock:
            if self._thread and self._thread.is_alive():
                return {"ok": False, "error": "步态已在运行",
                        "device_id": self.device_id}
            if not device_id:
                return {"ok": False, "error": "缺少 device_id"}
            self.device_id = device_id
            self.started_at = time.time()
            self.finished_at = None
            self.ticks = self.sent = self.errors = 0
            self.last_error = None
            self.last_result = None
            self._elapsed = 0.0
            self._fail_streak = 0
            self._stop = threading.Event()
            self._overrides = dict(overrides or {})
            self.angles = stand_angles(self._store_get() or {})
            self._thread = threading.Thread(target=self._loop, name="twin-gait",
                                            daemon=True)
            self._thread.start()
        if self._on_start:
            try:
                self._on_start(device_id)
            except Exception as e:  # noqa: BLE001
                print(f"[twin] 步态 on_start 回调失败：{e}", flush=True)
        print(f"[twin] 步态启动 device={device_id} "
              f"overrides={self._overrides or {}}", flush=True)
        return {"ok": True, "device_id": device_id, "state": self.state()}

    def stop(self, reason="user", wait=True, timeout=3.0):
        with self._lock:
            was = bool(self._thread and self._thread.is_alive())
            self._stop.set()
            th = self._thread
        if was and wait and th is not threading.current_thread():
            th.join(timeout)
        if self._on_stop:
            try:
                self._on_stop(self.device_id, reason)
            except Exception as e:  # noqa: BLE001
                print(f"[twin] 步态 on_stop 回调失败：{e}", flush=True)
        if was:
            print(f"[twin] 步态停止 device={self.device_id} reason={reason} "
                  f"ticks={self.ticks} sent={self.sent} errors={self.errors}",
                  flush=True)
        return {"ok": True, "was_running": was, "state": self.state()}

    # ---- 主循环 ----
    def _loop(self):
        dev = self.device_id
        last = time.time()
        stop_reason = "done"
        while not self._stop.is_set():
            t0 = time.time()
            dt = clamp(t0 - last, 0.001, 0.1)
            last = t0
            try:
                store = self._store_get() or {}
                gait = dict(store.get("gait") or {})
                gait.update({k: v for k, v in (self._overrides or {}).items()
                             if k in DEFAULT_GAIT})
                store = dict(store)
                store["gait"] = gait
                self._elapsed += dt
                angles = angles_at(store, self._elapsed)
                servos = angles_to_servos(store, angles)
                res = self._dispatch(dev, servos, gait.get("moveMs")) or {}
                with self._lock:
                    self.angles = angles
                    self.ticks += 1
                    self.last_result = {"ok": bool(res.get("ok")),
                                        "error": res.get("error")}
                    if res.get("ok"):
                        self.sent += 1
                        self._fail_streak = 0
                        self.last_error = None
                    else:
                        self.errors += 1
                        self._fail_streak += 1
                        self.last_error = res.get("error") or "下发失败"
            except Exception as e:  # noqa: BLE001
                with self._lock:
                    self.errors += 1
                    self._fail_streak += 1
                    self.last_error = str(e)
                print(f"[twin] 步态单拍异常：{e}", flush=True)
            with self._lock:
                if self._fail_streak >= self.MAX_CONSECUTIVE_FAIL:
                    stop_reason = "连续下发失败"
                    break
                deploy_ms = _int(gait.get("deployMs"), 120, 40, 2000)
            wait = deploy_ms / 1000.0 - (time.time() - t0)
            if wait > 0:
                self._stop.wait(wait)
        with self._lock:
            self.finished_at = time.time()
            if self.last_error:
                stop_reason = "异常：%s" % self.last_error
        self._stop.set()
        if stop_reason == "连续下发失败":
            print(f"[twin] 步态自动停止：{stop_reason}（{self.last_error}）", flush=True)
            if self._on_stop:
                try:
                    self._on_stop(dev, "error")
                except Exception:  # noqa: BLE001
                    pass


# ---------------------------------------------------------------- 动作组循环
class SequenceLooper(object):
    """动作组重复播放：服务端按估算时长续播，页面关掉也能一直跑。

    板端 sequence.run 在播放中会回 409 INVALID_STATE，所以续播要等一轮跑完。
    板端目前没有"动作完成事件"，只能按 sequence_duration_ms 估时长 + 余量。
    等板端补上完成事件后，这里可以改成事件驱动（见板端待补清单）。
    """

    def __init__(self, dispatch, on_start=None, on_stop=None):
        self._dispatch = dispatch          # dispatch(device_id, steps) -> res
        self._on_start = on_start
        self._on_stop = on_stop
        self._lock = threading.RLock()
        self._thread = None
        self._stop = threading.Event()
        self.device_id = None
        self.rounds = 0
        self.total_rounds = 1
        self.last_error = None
        self.finished_at = None
        self._external_stop = False

    def is_running(self, device_id=None):
        with self._lock:
            alive = bool(self._thread and self._thread.is_alive())
            if not alive:
                return False
            return device_id is None or self.device_id == device_id

    def state(self):
        with self._lock:
            return {"running": bool(self._thread and self._thread.is_alive()),
                    "device_id": self.device_id, "rounds": self.rounds,
                    "total_rounds": self.total_rounds,
                    "last_error": self.last_error}

    def start(self, device_id, steps, total_rounds=1, gap_ms=50):
        with self._lock:
            if self._thread and self._thread.is_alive():
                return {"ok": False, "error": "动作组循环已在运行"}
            self.device_id = device_id
            self.rounds = 0
            self.total_rounds = int(total_rounds)
            self.last_error = None
            self.finished_at = None
            self._stop = threading.Event()
            self._external_stop = False
            self._thread = threading.Thread(target=self._loop,
                                            args=(device_id, steps, total_rounds, gap_ms),
                                            name="twin-seq", daemon=True)
            self._thread.start()
        if self._on_start:
            try:
                self._on_start(device_id)
            except Exception as e:  # noqa: BLE001
                print(f"[twin] 动作组 on_start 回调失败：{e}", flush=True)
        return {"ok": True, "state": self.state()}

    def stop(self, wait=True, timeout=3.0):
        with self._lock:
            was = bool(self._thread and self._thread.is_alive())
            self._external_stop = True
            self._stop.set()
            th = self._thread
        if was and wait and th is not threading.current_thread():
            th.join(timeout)
        if self._on_stop:
            try:
                self._on_stop(self.device_id, "user")
            except Exception as e:  # noqa: BLE001
                print(f"[twin] 动作组 on_stop 回调失败：{e}", flush=True)
        return {"ok": True, "was_running": was, "state": self.state()}

    def _loop(self, dev, steps, total_rounds, gap_ms):
        per_round = sequence_duration_ms(steps) + max(0, int(gap_ms))
        while not self._stop.is_set() and (total_rounds <= 0 or self.rounds < total_rounds):
            res = self._dispatch(dev, steps) or {}
            if res.get("ok"):
                self.rounds += 1
            else:
                # sequence.run 是排他命令：板子还在跑上一轮时会回 409，退一步重试
                err = str(res.get("error") or "")
                if "INVALID_STATE" in err and self.rounds < max(1, total_rounds):
                    self._stop.wait(0.4)
                    continue
                with self._lock:
                    self.last_error = res.get("error") or "下发失败"
                break
            if total_rounds > 0 and self.rounds >= total_rounds:
                break
            self._stop.wait(max(0.05, per_round / 1000.0))
        with self._lock:
            self.finished_at = time.time()
            notify = not self._external_stop
        self._stop.set()
        if notify and self._on_stop:
            try:
                self._on_stop(self.device_id, "finished")
            except Exception:  # noqa: BLE001
                pass
