#!/usr/bin/env python3
"""PC 语音服务器（仅 ASR） —— 机器狗麦克风端 ESP32 的上位机。

协议（按需求规格）
-----------------
  mic ESP32  --(WebSocket 二进制, 纯 PCM, ?role=mic)-->  本服务器  --FunASR SenseVoice / faster-whisper-->  中文原文
  xiaozhi/dog ESP32  --(WebSocket OPUS, ?role=xiaozhi / ?role=dog)-->  本服务器  --解码→FunASR-->  中文原文
  browser UI  --(WebSocket JSON, ?role=dash)---------->  监测状态(电平/按钮/识别结果)

PCM 格式：16000 Hz、有符号 int16、小端、单声道、无压缩、无 WAV 头、无 JSON。
设备只在按住 GPIO9 按钮时发送 PCM，约每 20ms 发一帧（320 采样点 / 640 字节）。
服务器把收到的所有二进制消息拼接成连续 PCM 字节流，当约 300ms 没有新 PCM 时
判定“按钮松开 / 说话结束”，把整段音频提交一次识别（不会每帧都识别）。

本阶段不做：控制端(role=ctrl)、action JSON 转发、stop/move、TTS、
向控制端发音频、下发开始/停止采集命令。
"""
import argparse
import asyncio
import datetime
import json
import math
import os
import queue
import re
import socket
import threading
import time
import http.server

# 顶层重量级依赖改为惰性容错：缺 numpy/websockets 时不影响“仅控制服务”模式导入与运行。
try:
    import numpy as np
except Exception:  # noqa: BLE001
    np = None
try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None

import struct
import uuid

# ---- xiaozhi 采集端协议支持（OPUS + 二进制帧头 + hello 握手）----
# xiaozhi 固件默认把麦克风编码成 OPUS 经 WebSocket 上传（不是裸 PCM）。
# 握手：客户端先发 hello JSON -> 服务端回 hello；之后客户端用 listen start/stop
# 包裹的二进制 OPUS 帧。version=2/3 时二进制带帧头，version=0/1 为裸 OPUS。
try:
    # Windows 下 opuslib_next 需要 opus.dll。优先用项目目录自带的 opus.dll
    # （来自 conda-forge libopus 官方包）。做法：导入前把它所在目录注入本进程
    # PATH —— 只影响本进程，不改系统 PATH/注册表；删掉 opus.dll 即完全还原。
    if os.name == "nt":
        _proj_dir = os.path.dirname(os.path.abspath(__file__))
        if os.path.exists(os.path.join(_proj_dir, "opus.dll")):
            os.environ["PATH"] = _proj_dir + os.pathsep + os.environ.get("PATH", "")
    import opuslib_next
    _OPUS_DECODER = lambda sr=16000, ch=1: opuslib_next.Decoder(sr, ch)
    _OPUS_ENCODER = lambda sr=16000, ch=1, app="voip": opuslib_next.Encoder(sr, ch, app)
except Exception:  # noqa: BLE001
    opuslib_next = None
    _OPUS_DECODER = None
    _OPUS_ENCODER = None

XZ_OPUS_SR = 16000          # xiaozhi 上传的 OPUS 采样率
XZ_FRAME_MS = 60            # OPUS 帧长（与固件 OPUS_FRAME_DURATION_MS 一致）
XZ_FRAME_SAMPLES = XZ_OPUS_SR * XZ_FRAME_MS // 1000   # 960 采样/帧 = 1920 字节 PCM

# ===========================================================================
# 代码段导航（在本文件内检索关键词定位）
#   [A] 配置与全局状态        : 下方“可配置参数”段（PORT / 引擎 / 全局变量）
#   [B] 语音 ASR 服务(WS:8765) : [B] 段（Denoiser→transcribe→asr_worker→handler/monitor→xiaozhi 协议）
#   [C] 设备控制/OTA(HTTP:8000): [C] CTRL_SERVER 段（状态/加载器/队列/路由/启动）
#   [D] 入口 main()            : [D] 段（统一编排两套服务的启动）
# ===========================================================================

# ---- 可配置参数（命令行可覆盖） ------------------------------------------
PORT = 8765             # WebSocket 端口（麦克风 / 监测）
HTTP_PORT = 8080        # 监测 GUI 端口（已废弃：语音监测台改由控制服务 :8000/voice 提供）
SAMPLE_RATE = 16000
SAMPLE_BYTES = 2        # int16
FRAME_SAMPLES = 320     # 20ms @ 16k
FRAME_BYTES = FRAME_SAMPLES * SAMPLE_BYTES
BUTTON_GAP_S = 0.30     # 连续这么久没 PCM => 判定说话结束
MONITOR_TICK = 0.05     # 间隙检测 / 状态广播节拍
MODEL = "base"
DEVICE = "cpu"
COMPUTE = "int8"
ENGINE = "funasr"      # ASR 引擎：funasr=SenseVoice（中文抗噪强）/ whisper=faster-whisper

clients = {"mic": set(), "dash": set(), "xz": set(), "dog": set()}   # xz = xiaozhi 采集端；dog = 机器狗专用采集端（协议同 xiaozhi）
mic_state = {}          # ws -> {"buf","last","recording","level"}
xz_state = {}           # ws -> xiaozhi 会话状态（version/sample_rate/buf/recording...）
asr = None
asr_queue: "queue.Queue" = queue.Queue()
LOOP = None
ASR_INFO = {"status": "loading", "model": MODEL, "note": ""}  # 监测台展示模型状态
SAVE_WAV = False          # --save-wav：把每段提交的音频存成 wav，方便排查采集问题
DENOISE = True           # 软件降噪（50/100/150Hz 陷波 + 高低通）默认开
VAD = True                # Whisper VAD 语音活动检测；强电流麦下建议 --no-vad


# ===========================================================================
# [B] 语音 ASR 服务（WebSocket :8765）
# 四类客户端：?role=mic（裸 PCM）/ ?role=xiaozhi（OPUS）/ ?role=dog（机器狗 OPUS，同 xiaozhi 协议）/ ?role=dash（监测）
# 识别管线：Denoiser 降噪 -> transcribe -> asr_worker(阻塞线程) -> broadcast_dash
# 本段与 [C] 控制服务零耦合，仅共享进程与 main() 启动时序。
# ===========================================================================

# ---------------------------------------------------------------------------
# 软件降噪（仅在 --no-denoise 未指定时启用）
# 目标：压 INMP441 的 50Hz 工频电流麦（电源/共地噪声）+ 直流偏移。
# 实现：一阶 DC 阻断高通 + RBJ 50Hz 陷波，逐样本递推（数值稳定）。
# ---------------------------------------------------------------------------
class _Biquad:
    """二阶 IIR 滤波器（Direct Form I）。"""
    def __init__(self, b, a):
        self.b0, self.b1, self.b2 = b
        self.a1, self.a2 = a[1], a[2]
        self.x1 = self.x2 = 0.0
        self.y1 = self.y2 = 0.0

    def process(self, x: float) -> float:
        y = (self.b0 * x + self.b1 * self.x1 + self.b2 * self.x2
             - self.a1 * self.y1 - self.a2 * self.y2)
        self.x2, self.x1 = self.x1, x
        self.y2, self.y1 = self.y1, y
        return y


def _rbj_notch(fs, f0, q):
    w0 = 2.0 * math.pi * f0 / fs
    alpha = math.sin(w0) / (2.0 * q)
    cw = math.cos(w0)
    b = [1.0, -2.0 * cw, 1.0]
    a = [1.0 + alpha, -2.0 * cw, 1.0 - alpha]
    return [b[i] / a[0] for i in range(3)], [1.0, a[1] / a[0], a[2] / a[0]]


def _rbj_hp(fs, fc, q=0.7071):
    w0 = 2.0 * math.pi * fc / fs
    alpha = math.sin(w0) / (2.0 * q)
    cw = math.cos(w0)
    b = [(1.0 + cw) / 2.0, -(1.0 + cw), (1.0 + cw) / 2.0]
    a = [1.0 + alpha, -2.0 * cw, 1.0 - alpha]
    return [b[i] / a[0] for i in range(3)], [1.0, a[1] / a[0], a[2] / a[0]]


def _rbj_lp(fs, fc, q=0.7071):
    w0 = 2.0 * math.pi * fc / fs
    alpha = math.sin(w0) / (2.0 * q)
    cw = math.cos(w0)
    b = [(1.0 - cw) / 2.0, 1.0 - cw, (1.0 - cw) / 2.0]
    a = [1.0 + alpha, -2.0 * cw, 1.0 - alpha]
    return [b[i] / a[0] for i in range(3)], [1.0, a[1] / a[0], a[2] / a[0]]


class Denoiser:
    def __init__(self, fs=SAMPLE_RATE):
        # 滤波链（顺序执行）：
        #  ① 100Hz 高通：去直流 + 低频隆隆；
        #  ② 50/100/150Hz 工频陷波(Q=8)：电流麦多来自半波整流电源，富含 50Hz 及其谐波；
        #  ③ 7500Hz 低通：去掉 8k 以上 RF / 电源宽带噪声（Whisper 有效频带 ~8kHz 以内）。
        self.stages = []
        hb, ha = _rbj_hp(fs, 100.0)
        self.stages.append(_Biquad(hb, ha))
        for f0 in (50.0, 100.0, 150.0):
            b, a = _rbj_notch(fs, f0, 8.0)
            self.stages.append(_Biquad(b, a))
        lb, la = _rbj_lp(fs, 7500.0)
        self.stages.append(_Biquad(lb, la))
        self.gain = 1.2  # 轻微补偿（各陷波/高低通在通带内基本平直，留点余量）

    def process(self, audio: np.ndarray) -> np.ndarray:
        out = np.empty(audio.size, dtype=np.float64)
        for i in range(audio.size):
            x = float(audio[i])
            for s in self.stages:
                x = s.process(x)
            out[i] = x * self.gain
        np.clip(out, -1.0, 1.0, out)
        return out.astype(np.float32)


_DENOISER = None


def denoise_audio(audio: np.ndarray) -> np.ndarray:
    global _DENOISER
    if _DENOISER is None:
        _DENOISER = Denoiser(SAMPLE_RATE)
    return _DENOISER.process(audio)


def float_to_pcm16(audio: np.ndarray) -> bytes:
    """float(-1~1) -> int16 字节流，用于写 wav。"""
    a = np.clip(audio, -1.0, 1.0)
    return (a * 32767.0).astype(np.int16).tobytes()


# ---------------------------------------------------------------------------
# ASR（faster-whisper，中文）
# ---------------------------------------------------------------------------
def load_asr():
    """按 ENGINE 加载 ASR 模型。funasr=SenseVoice（中文抗噪强）；whisper=faster-whisper。"""
    global asr, ASR_INFO
    if ENGINE == "funasr":
        try:
            from funasr import AutoModel
            model_id = MODEL
            if model_id in ("base", "tiny", "small", "medium", "large"):
                model_id = "iic/SenseVoiceSmall"   # 未显式指定时给默认中文模型
            asr = AutoModel(
                model=model_id,
                trust_remote_code=True,
                vad_model="fsmn-vad",
                device="cpu",          # 无 GPU 时固定 cpu
                disable_update=True,   # 离线优先，不让它联网查更新
            )
            ASR_INFO = {"status": "ready", "model": model_id, "note": ""}
            print(f"[asr] FunASR ready (model={model_id}, cpu)")
        except Exception as e:  # noqa: BLE001
            asr = None
            ASR_INFO = {
                "status": "unavailable", "model": MODEL,
                "note": ("FunASR 加载失败，识别将返回空。最常见原因：\n"
                         "  ① torch 未安装（funasr 运行必需，但 pip 不自动装）：\n"
                         "     pip install torch torchaudio --index-url https://download.pytorch.org/whl/cpu\n"
                         "  ② 模型 iic/SenseVoiceSmall 首次需从 ModelScope 下载（需联网）。\n"
                         "临时兜底：python server.py --engine whisper --model models/faster-whisper-base")
            }
            print("[asr] FunASR 不可用：", e)
        return

    # ---- whisper 引擎 ----
    try:
        from faster_whisper import WhisperModel
        asr = WhisperModel(MODEL, device=DEVICE, compute_type=COMPUTE)
        ASR_INFO = {"status": "ready", "model": MODEL, "note": ""}
        print(f"[asr] faster-whisper ready (model={MODEL}, {DEVICE}/{COMPUTE})")
    except Exception as e:  # noqa: BLE001
        asr = None
        ASR_INFO = {
            "status": "unavailable", "model": MODEL,
            "note": ("模型加载失败。本项目已在 models/faster-whisper-base/ 预置 base 模型，推荐直接本地启动：\n"
                     "  python server.py --model models/faster-whisper-base\n"
                     "  或双击 start_base.bat\n"
                     "也可从 HuggingFace 手动下载后指定路径：python server.py --model <本地模型目录>")
        }
        print("[asr] faster-whisper 不可用：", e)
        print("       识别将返回空。修复方法见上方 note（也可 python server.py --model tiny 减小下载量）。")


def transcribe(pcm: bytes) -> str:
    """int16 PCM 字节 -> 中文文本。PCM 只能还原采样，文字必须靠 ASR 模型。"""
    if asr is None or len(pcm) < FRAME_BYTES:
        return ""
    # float_sample = int16_sample / 32768.0  （范围约 -1.0 ~ 1.0）
    audio = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0
    if DENOISE:
        audio = denoise_audio(audio)   # 抑制 50Hz 工频电流麦 + 直流偏移
    if ENGINE == "funasr":
        import os
        import tempfile
        import soundfile as sf
        tmp = tempfile.mktemp(suffix=".wav")
        try:
            sf.write(tmp, audio, SAMPLE_RATE)
            res = asr.generate(input=[tmp], language="auto", use_itn=True, batch_size_s=60)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)
        if not res:
            return ""
        text = res[0].get("text", "") if isinstance(res[0], dict) else str(res[0])
        try:
            from funasr.utils.post_process_utils import rich_transcription_postprocess
            text = rich_transcription_postprocess(text)
        except Exception:  # noqa: BLE001
            pass
        # 兜底：SenseVoice 输出可能带 <|zh|><|NEUTRAL|><|Speech|><|withitn|> 等内部标记，
        # 上面的官方后处理在部分 funasr 版本中导入失败，这里用正则强制清掉，
        # 否则标记会原样回传到板子 LCD 显示。
        text = re.sub(r"<\|[^|>]*\|>", "", text)
        return text.strip()
    segs, _ = asr.transcribe(audio, language="zh", vad_filter=VAD)
    return "".join(s.text for s in segs).strip()


def downsample_wave(pcm: bytes, points: int):
    """把一段 PCM 降采样成 points 个峰值（0~1），供 GUI 画波形。"""
    arr = np.frombuffer(pcm, dtype=np.int16)
    if arr.size == 0:
        return []
    n = min(points, arr.size)
    bucket = max(1, arr.size // n)
    out = []
    for i in range(0, arr.size, bucket):
        chunk = arr[i:i + bucket]
        peak = np.max(np.abs(chunk)) if chunk.size else 0
        out.append(round(float(peak) / 32768.0, 4))
    return out[:points]


# ---------------------------------------------------------------------------
# PC 语音输入模式（--pc-input）：识别结果写剪贴板并模拟 Ctrl+V 粘贴到
# 当前焦点窗口的光标处。纯 Windows ctypes 实现，不额外装包。
# 未开启该 flag 时完全不触发，行为与原版一致。
# ---------------------------------------------------------------------------
PC_INPUT = False
PC_INPUT_CONFIRM = False        # 确认模式：识别后先缓存，等板子 confirm 才发送（防误输入）
pending_pc_input = {}           # ws -> 待确认文本（确认模式）
pending_lock = threading.Lock()

if os.name == "nt":
    import ctypes
    from ctypes import wintypes

    _user32 = ctypes.WinDLL("user32", use_last_error=True)
    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    # 句柄是 64 位指针，必须显式声明 restype/argtypes，否则被截断成 int 导致访问违例
    _kernel32.GlobalAlloc.restype = ctypes.c_void_p
    _kernel32.GlobalAlloc.argtypes = [wintypes.UINT, ctypes.c_size_t]
    _kernel32.GlobalLock.restype = ctypes.c_void_p
    _kernel32.GlobalLock.argtypes = [ctypes.c_void_p]
    _kernel32.GlobalUnlock.argtypes = [ctypes.c_void_p]
    _user32.SetClipboardData.restype = ctypes.c_void_p
    _user32.SetClipboardData.argtypes = [wintypes.UINT, ctypes.c_void_p]
    # GetClipboardData 同样返回 64 位句柄，必须声明 restype，否则 Win64 下截断成 int 触发访问违例
    _user32.GetClipboardData.restype = ctypes.c_void_p
    _user32.GetClipboardData.argtypes = [wintypes.UINT]

    _CF_UNICODETEXT = 13
    _GMEM_MOVEABLE = 0x0002
    _INPUT_KEYBOARD = 1
    _KEYEVENTF_KEYUP = 0x0002
    _VK_CONTROL, _VK_V = 0x11, 0x56

    class _KEYBDINPUT(ctypes.Structure):
        _fields_ = [("wVk", wintypes.WORD), ("wScan", wintypes.WORD),
                    ("dwFlags", wintypes.DWORD), ("time", wintypes.DWORD),
                    ("dwExtraInfo", ctypes.POINTER(wintypes.ULONG))]

    class _INPUT(ctypes.Structure):
        # type(4) + 4 对齐 + KEYBDINPUT(24) + 8 填充 = 40 字节，与 Win64 INPUT 一致
        _fields_ = [("type", wintypes.DWORD), ("ki", _KEYBDINPUT),
                    ("pad", ctypes.c_ubyte * 8)]

    def _win_set_clipboard_text(text: str) -> bool:
        h = None
        if not _user32.OpenClipboard(None):
            return False
        try:
            _user32.EmptyClipboard()
            data = ctypes.create_unicode_buffer(text)
            h = _kernel32.GlobalAlloc(_GMEM_MOVEABLE, ctypes.sizeof(data))
            if not h:
                return False
            p = _kernel32.GlobalLock(h)
            if not p:
                return False
            ctypes.memmove(p, data, ctypes.sizeof(data))
            _kernel32.GlobalUnlock(h)
            return bool(_user32.SetClipboardData(_CF_UNICODETEXT, h))
        finally:
            _user32.CloseClipboard()

    def _win_get_clipboard_text() -> str:
        """读取当前剪贴板文字（用于粘贴后还原）。无文字/不可读返回空。"""
        if not _user32.OpenClipboard(None):
            return ""
        try:
            h = _user32.GetClipboardData(_CF_UNICODETEXT)
            if not h:
                return ""
            p = _kernel32.GlobalLock(h)
            if not p:
                return ""
            try:
                return ctypes.wstring_at(p)
            finally:
                _kernel32.GlobalUnlock(h)
        except Exception:  # noqa: BLE001
            return ""
        finally:
            _user32.CloseClipboard()

    def _win_press_ctrl_v():
        def key(vk, up=False):
            inp = _INPUT()
            inp.type = _INPUT_KEYBOARD
            inp.ki.wVk = vk
            inp.ki.dwFlags = _KEYEVENTF_KEYUP if up else 0
            return _user32.SendInput(1, ctypes.byref(inp), ctypes.sizeof(_INPUT))
        key(_VK_CONTROL)
        key(_VK_V)
        key(_VK_V, up=True)
        key(_VK_CONTROL, up=True)

    def pc_input_inject(text: str):
        """把识别文本粘贴到当前焦点窗口。先备份用户原有剪贴板，粘贴后还原，
        避免每次说话都冲掉用户复制的内容（P0 修复）。失败只打日志，不影响识别主流程。"""
        if not text:
            return
        try:
            prev = _win_get_clipboard_text()   # ① 备份当前剪贴板文字
            if not _win_set_clipboard_text(text):
                print("[pc-input] 剪贴板写入失败，跳过粘贴")
                return
            time.sleep(0.05)                    # 剪贴板落盘极短缓冲
            _win_press_ctrl_v()                 # ② Ctrl+V 粘贴
            if prev:                            # ③ 还原用户原来的剪贴板
                time.sleep(0.10)
                _win_set_clipboard_text(prev)
            print(f"[pc-input] 已粘贴到焦点窗口：{text!r}")
        except Exception as e:  # noqa: BLE001
            print(f"[pc-input] 注入失败：{e}")
else:
    def pc_input_inject(text: str):
        pass


def asr_worker():
    """后台线程：顺序消费识别队列，避免并发转录抢占。"""
    while True:
        item = asr_queue.get()
        if item is None:
            asr_queue.task_done()
            break
        ws, pcm, duration_ms = item
        t0 = time.time()
        try:
            text = transcribe(pcm)
        except Exception as e:  # noqa: BLE001
            text = ""
            print(f"[asr] 识别异常：{e}")
        dt = int((time.time() - t0) * 1000)
        # 若来自 xiaozhi 采集端，把识别结果回传板子 LCD 显示（type=stt）
        if text and xz_state.get(ws) is not None:
            sid = xz_state[ws].get("session_id", "")
            if LOOP is not None:
                asyncio.run_coroutine_threadsafe(
                    _send_json(ws, {"type": "stt", "text": text, "session_id": sid}), LOOP)
        print(f"[asr] 识别结果：{text if text else '(空)'}（耗时 {dt}ms）")

        # [C-7] 大脑层开了就把这句话交给大模型（后台线程，不阻塞识别队列）
        if BRAIN is not None and text:
            did = _device_id_by_ws(ws) or os.environ.get("DOG_BRAIN_DEVICE")
            if did:
                brain_say(did, text)
            else:
                print("[brain] 该连接未绑定设备，跳过（可用 DOG_BRAIN_DEVICE 指定默认设备）",
                      flush=True)

        if PC_INPUT and text:
            if PC_INPUT_CONFIRM:
                # 确认模式：先缓存，等板子发 confirm 才注入，避免识别错误直接灌进窗口
                with pending_lock:
                    pending_pc_input[ws] = text
                print(f"[pc-input] 已识别，等待确认发送：{text!r}")
                if LOOP is not None:
                    asyncio.run_coroutine_threadsafe(
                        broadcast_dash({"type": "event",
                                        "msg": f"待确认发送：{text}（板子按确认键）"}), LOOP)
            else:
                pc_input_inject(text)  # PC 语音输入模式：直接粘贴到焦点窗口
        payload = {
            "type": "result",
            "text": text,
            "duration_ms": duration_ms,
            "asr_ms": dt,
            "ts": time.strftime("%H:%M:%S"),
            "wave": downsample_wave(pcm, 140) if text else [],
        }
        if LOOP is not None:
            asyncio.run_coroutine_threadsafe(broadcast_dash(payload), LOOP)
        asr_queue.task_done()


# ---------------------------------------------------------------------------
# 广播（监测端）
# ---------------------------------------------------------------------------
async def broadcast_dash(payload: dict):
    msg = json.dumps(payload, ensure_ascii=False)
    for ws in list(clients["dash"]):
        try:
            await ws.send(msg)
        except Exception:  # noqa: BLE001
            clients["dash"].discard(ws)


async def broadcast_asr_status():
    """把当前 ASR 模型状态推给监测台（含加载失败原因）。"""
    await broadcast_dash({"type": "asr", **ASR_INFO})


def mic_connected() -> bool:
    return len(clients["mic"]) > 0 or len(xz_state) > 0


# ---------------------------------------------------------------------------
# PCM 工具
# ---------------------------------------------------------------------------
def frame_level(message: bytes) -> float:
    """单帧 RMS 归一化到 0~1，用于 VU 表。"""
    arr = np.frombuffer(message, dtype=np.int16)
    if arr.size == 0:
        return 0.0
    rms = float(np.sqrt(np.mean(arr.astype(np.float32) ** 2)))
    return min(1.0, rms / 6000.0)


def write_wav(pcm: bytes, tag: str = ""):
    """把一段 int16 PCM 字节存成 WAV（16k/16bit/mono），用于人工听测排查。"""
    if not pcm or len(pcm) < 4:
        return
    import wave
    ts = time.strftime("%Y%m%d_%H%M%S")
    fn = f"record_{ts}{('_' + tag) if tag else ''}.wav"
    try:
        with wave.open(fn, "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)          # int16
            w.setframerate(SAMPLE_RATE)
            w.writeframes(pcm)
        print(f"[wav] 已保存 {fn}  ({len(pcm)//2} 采样 / {len(pcm)//2/SAMPLE_RATE:.2f}s)")
    except Exception as e:  # noqa: BLE001
        print(f"[wav] 保存失败：{e}")


# ---------------------------------------------------------------------------
# WebSocket 中枢
# ---------------------------------------------------------------------------
async def handler(ws, path=None):
    req_path = getattr(getattr(ws, "request", None), "path", path) or ""
    if "role=xiaozhi" in req_path:
        role = "xz"
    elif "role=dog" in req_path:
        role = "dog"
    elif "role=mic" in req_path:
        role = "mic"
    elif "role=dash" in req_path:
        role = "dash"
    elif "role=" in req_path:
        print(f"[ws] 拒绝未知 role：{req_path}")
        await ws.close(1008, "unknown role")
        return
    else:
        role = "dash"  # 浏览器不带 role 默认识别为监测端；若收到 hello 则自动升级为 xiaozhi 设备

    clients[role].add(ws)
    print(f"[{role}] 已连接（mic={len(clients['mic'])} xz+dog={len(xz_state)} dash={len(clients['dash'])}）")

    if role == "dash":
        await broadcast_dash({
            "type": "status", "mic": "connected" if mic_connected() else "disconnected",
            "button": False, "recording": False, "level": 0.0, "buffer_ms": 0,
        })
        await broadcast_asr_status()
        await broadcast_mic_status()   # 主动补推一次连接状态（前端可能晚于事件连入）

    is_xz = (role in ("xz", "dog"))
    mon = None
    if role == "mic":
        mic_state[ws] = {"buf": bytearray(), "last": 0.0, "recording": False, "level": 0.0, "ending": False}
        mon = asyncio.create_task(monitor(ws))
        await broadcast_dash({"type": "event", "msg": "麦克风已连接"})
        await broadcast_mic_status()
    elif is_xz:
        label = "dog 采集端" if role == "dog" else "xiaozhi 采集端"
        xz_state[ws] = {"buf": bytearray(), "last": 0.0, "recording": False,
                        "level": 0.0, "ending": False, "mode": role, "version": 0}
        await broadcast_dash({"type": "event", "msg": f"{label} 已连接，等待 hello"})
        await broadcast_mic_status()

    try:
        async for message in ws:
            if is_xz:  # 调试：打印该连接前 30 条原始消息（文本原文 / 二进制长度+帧头）
                st_dbg = xz_state.get(ws)
                if st_dbg is not None:
                    st_dbg["dbg_n"] = st_dbg.get("dbg_n", 0) + 1
                    if st_dbg["dbg_n"] <= 30:
                        if isinstance(message, (bytes, bytearray)):
                            prev = f"binary {len(message)}B head={bytes(message[:8]).hex()}"
                        else:
                            prev = f"text {len(message)}B: {message[:200]!r}"
                        msg_dbg = f"[{role}-dbg #{st_dbg['dbg_n']}] {prev}"
                        print(msg_dbg, flush=True)
                        await broadcast_dash({"type": "event", "msg": msg_dbg})
            if isinstance(message, (bytes, bytearray)):
                if role == "mic":
                    st = mic_state.get(ws)
                    if st is None:
                        continue
                    # 拼接所有二进制消息为连续 PCM 流；不依赖 WS 消息边界
                    st["buf"] += bytes(message)
                    st["last"] = time.monotonic()
                    st["level"] = frame_level(message)
                    if not st["recording"]:
                        st["recording"] = True
                        print("[mic] 按钮按下（开始说话）")
                        await broadcast_dash({"type": "event", "msg": "开始说话（按钮按下）"})
                        await broadcast_mic_status()
                elif is_xz:
                    await xz_handle_binary(ws, bytes(message))
            elif isinstance(message, str):
                if is_xz:
                    await xz_handle_text(ws, message)
                else:
                    # 浏览器监测台不发文本；若收到 hello 则自动升级为 xiaozhi 设备
                    try:
                        j = json.loads(message)
                    except Exception:  # noqa: BLE001
                        j = None
                    if j and j.get("type") == "hello":
                        is_xz = True
                        clients["xz"].add(ws)
                        xz_state[ws] = {"buf": bytearray(), "last": 0.0, "recording": False,
                                        "level": 0.0, "ending": False, "mode": "xz", "version": 0}
                        await xz_handle_hello(ws, j)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if role == "mic":
            if mon is not None:
                mon.cancel()
            mic_state.pop(ws, None)
            await broadcast_dash({"type": "event", "msg": "麦克风断开，未完成的语音段丢弃"})
            await broadcast_mic_status()
        if is_xz or ws in xz_state:
            xz_state.pop(ws, None)
            did_gone = WS_DEVICE.pop(ws, None)          # 先取出再解绑，便于打印
            dev_unbind_ws(did_gone, ws)                 # 解绑下行目标
            with pending_lock:
                pending_pc_input.pop(ws, None)   # 清理该连接的待确认文本，避免内存泄漏
            clients["xz"].discard(ws)
            clients["dog"].discard(ws)
            label = "dog 采集端" if role == "dog" else "xiaozhi 采集端"
            print(f"[{role}] 断开 device={did_gone or ip} ({label})", flush=True)
            await broadcast_dash({"type": "event", "msg": f"{label} 断开"})
            await broadcast_mic_status()
        clients[role].discard(ws)


async def monitor(ws):
    """每 MONITOR_TICK 检查一次：广播状态；根据“无 PCM 静默”判定说话结束。

    间隙判定做了容错：超过软阈值（BUTTON_GAP_S）只标记“疑似结束”，
    只有静默持续到硬阈值（吸收 WiFi 突发造成的短暂断流）才真正提交；
    期间若又有数据到达则自动恢复，避免把一次正常说话误切成多段。
    """
    while True:
        await asyncio.sleep(MONITOR_TICK)
        st = mic_state.get(ws)
        if st is None:
            return
        now = time.monotonic()
        buffer_ms = int(len(st["buf"]) / SAMPLE_BYTES / SAMPLE_RATE * 1000)
        await broadcast_dash({
            "type": "status",
            "mic": "connected",
            "button": st["recording"],
            "recording": st["recording"],
            "level": st["level"],
            "buffer_ms": buffer_ms,
        })
        if st["recording"]:
            gap = now - st["last"]
            hard_gap = max(BUTTON_GAP_S + 0.5, 0.8)  # 硬阈值：吸收 WiFi 突发，避免误切
            if gap >= BUTTON_GAP_S:
                # 超过软阈值：可能是松开按钮，也可能是 WiFi 突发造成的短暂断流
                if not st.get("ending", False):
                    st["ending"] = True
                    print(f"[mic] 疑似说话结束（静默 {gap * 1000:.0f}ms，等待确认）")
                    await broadcast_dash({"type": "event",
                                          "msg": f"疑似结束（静默 {gap * 1000:.0f}ms，等待确认）"})
                if gap >= hard_gap:
                    # 静默确实持续够久 -> 真正结束，提交整段（不是每帧识别）
                    duration_ms = int(len(st["buf"]) / SAMPLE_BYTES / SAMPLE_RATE * 1000)
                    await broadcast_dash({"type": "event", "msg": f"提交识别（{duration_ms} ms）"})
                    submit_buffer(ws, st)
            else:
                # 又收到数据 -> 之前的静默是 WiFi 突发，取消“疑似结束”，继续采集
                if st.get("ending", False):
                    st["ending"] = False
                    await broadcast_dash({"type": "event", "msg": "恢复采集中（WiFi 突发结束）"})


async def broadcast_mic_status():
    await broadcast_dash({
        "type": "micstatus",
        "connected": mic_connected(),
        "count": len(clients["mic"]) + len(xz_state),
    })


# ---------------------------------------------------------------------------
# xiaozhi 采集端协议适配
# ---------------------------------------------------------------------------
async def _send_json(ws, obj):
    """给指定 ws 发送 JSON（通常回给采集端板子的指令）。"""
    try:
        text = json.dumps(obj, ensure_ascii=False)
        await ws.send(text)
        print(f"[xz->] 发送: {text[:200]}", flush=True)
    except Exception:  # noqa: BLE001
        pass


def xz_strip_header(version, data):
    """按协商的协议版本剥掉二进制帧头，返回纯 OPUS 载荷。
    version=2 -> BinaryProtocol2（16 字节头）；version=3 -> BinaryProtocol3（4 字节头）；
    其余（0/1 等）为裸 OPUS，不剥头。"""
    if version == 2:
        return data[16:] if len(data) >= 16 else None
    if version == 3:
        return data[4:] if len(data) >= 4 else None
    return data


def xz_decode_opus(st, opus_bytes):
    """OPUS 载荷 -> 16k/int16/mono PCM 字节。每帧 960 采样。"""
    if _OPUS_DECODER is None or not opus_bytes:
        return None
    try:
        if st.get("opus_dec") is None:
            st["opus_dec"] = _OPUS_DECODER(XZ_OPUS_SR, 1)
        n = st.get("sample_rate", XZ_OPUS_SR) * st.get("frame_duration", XZ_FRAME_MS) // 1000
        return st["opus_dec"].decode(bytes(opus_bytes), n)
    except Exception as e:  # noqa: BLE001
        print(f"[xz] OPUS 解码失败：{e}")
        return None


async def xz_handle_hello(ws, j):
    """处理客户端 hello，回 hello（含 session_id + audio_params）。"""
    st = xz_state.get(ws) or xz_state.setdefault(ws, {"buf": bytearray(), "recording": False})
    st["version"] = int(j.get("version", 0))
    ap = j.get("audio_params") or {}
    sr = int(ap.get("sample_rate", XZ_OPUS_SR))
    fd = int(ap.get("frame_duration", XZ_FRAME_MS))
    st["sample_rate"] = sr
    st["frame_duration"] = fd
    st["session_id"] = str(uuid.uuid4())
    # 自动登记设备端点（新版双向架构）：用 WS 源 IP 作为 ESP32 的 HTTP 主机，供控制台下发指令时直推
    try:
        ra = getattr(ws, "remote_address", None)
        src_ip = ra[0] if isinstance(ra, (tuple, list)) and ra else None
        # hello 里带 device_id 最优；没有就用源 IP 兜底，保证下行仍有目标
        did = j.get("device_id") or j.get("id") or src_ip
        http_port = int(j.get("http_port") or 80)
        if did:
            st["device_id"] = did
            WS_DEVICE[ws] = did
            dev_bind_ws(did, ws, session_id=st["session_id"], ip=src_ip)
            if src_ip:
                device_endpoint_register(did, src_ip, http_port, auto=True)
                print(f"[ctrl] 设备 {did} 已登记 -> {src_ip}:{http_port}")
            # hello 里若顺带写了固件版本/内存/信号等，一并记下来
            metrics = pick_metrics(j)
            if metrics:
                dev_set_state(did, "IDLE", ip=src_ip, metrics=metrics)
                print(f"[ctrl] 设备 {did} 上报运行数据 {metrics}", flush=True)
    except Exception:  # noqa: BLE001
        pass
    hello = {
        "type": "hello",
        "version": 1,           # 对齐官方服务端回包（携带 version 字段）
        "transport": "websocket",
        "session_id": st["session_id"],
        "audio_params": {"format": "opus", "sample_rate": sr,
                         "channels": 1, "frame_duration": fd},
    }
    await _send_json(ws, hello)
    await broadcast_dash({"type": "event", "msg": f"xiaozhi 已握手（v{st['version']}, {sr}/{fd}ms）"})
    await broadcast_mic_status()
    print(f"[xz] hello: version={st['version']} audio={sr}Hz/{fd}ms")


async def xz_handle_text(ws, message):
    """处理 xiaozhi 文本指令：listen start/stop、abort。"""
    try:
        j = json.loads(message)
    except Exception:  # noqa: BLE001
        return
    st = xz_state.get(ws)
    if st is None:
        return
    t = j.get("type")
    if t == "pc_input":
        # 确认模式下的发送控制：confirm 才把缓存文本注入 PC 焦点窗口，cancel 丢弃。
        # 板子端识别结果已通过 stt 显示在 LCD，用户确认无误后再按“发送键”发此指令。
        action = j.get("action")
        with pending_lock:
            pend = pending_pc_input.get(ws)
        if action == "confirm":
            if pend:
                # 注入会短暂占用剪贴板+模拟按键，放后台线程，不阻塞 WS 事件循环
                threading.Thread(target=pc_input_inject, args=(pend,), daemon=True).start()
                with pending_lock:
                    pending_pc_input.pop(ws, None)
                await _send_json(ws, {"type": "pc_input_ack", "action": "confirm", "ok": True})
                print(f"[pc-input] 已确认发送：{pend!r}")
            else:
                await _send_json(ws, {"type": "pc_input_ack", "action": "confirm",
                                      "ok": False, "note": "无待发送文本"})
        elif action == "cancel":
            with pending_lock:
                pending_pc_input.pop(ws, None)
            await _send_json(ws, {"type": "pc_input_ack", "action": "cancel", "ok": True})
            print("[pc-input] 已取消发送")
        else:
            await _send_json(ws, {"type": "pc_input_ack", "action": action,
                                  "ok": False, "note": "未知 action"})
        return
    if t == "hello":
        # 关键：板子连接后先发 hello，必须回 hello（带 session_id）它才认为通道建立，
        # 否则板端会一直 Start listening failed: ESP_ERR_INVALID_STATE
        await xz_handle_hello(ws, j)
        return
    if t == "command_result":
        # 板子对 WS 控制指令的回执，唤醒等待中的同步调用
        ws_on_command_result(j)
        return
    if t == "state":
        # 板子周期状态上报（平时走这条；OTA 阶段 WS 可能已停，改走 HTTP /state）
        did = j.get("device_id") or st.get("device_id")
        if did:
            metrics = pick_metrics(j)
            # 板端目前不单独发 state 字段，只回 servo_busy / sequence_playing 这类布尔量，
            # 这里按它推导，省得每来一条就把它打回 IDLE（那会误放掉动作占用，后续命令判空）。
            cur = str(j.get("state") or j.get("mode") or "").upper()
            if cur not in DEV_STATES:
                if j.get("servo_busy") or j.get("sequence_playing"):
                    cur = "MOVING"
                elif j.get("wifi_connected") is False:
                    cur = "ERROR"
                else:
                    cur = "IDLE"
            dev_set_state(did, cur, ip=(j.get("ip") or None), metrics=metrics or None)
        return
    if t == "listen":
        state = j.get("state")
        if state == "start":
            st["buf"] = bytearray()
            st["recording"] = True
            st["ending"] = False
            st["last"] = time.monotonic()
            print("[xz] 开始说话（listen start）")
            if st.get("device_id"):
                dev_set_state(st["device_id"], "LISTENING")
            await broadcast_dash({"type": "event", "msg": "开始说话（按键/唤醒）"})
            await broadcast_mic_status()
        elif state == "stop":
            print("[xz] 停止（listen stop）")
            await broadcast_dash({"type": "event", "msg": "提交识别（listen stop）"})
            submit_buffer(ws, st)
            if st.get("device_id"):
                dev_set_state(st["device_id"], "IDLE")
        # state=="detect" 为唤醒词事件，无需处理
    elif t == "abort":
        if st["recording"]:
            submit_buffer(ws, st)


async def xz_handle_binary(ws, data):
    """处理 xiaozhi 二进制 OPUS 帧 -> 解码 -> 累积到 PCM 缓冲。"""
    st = xz_state.get(ws)
    if st is None:
        return
    if not st["recording"]:
        # 某些模式下板子先发音频后发 listen start：首帧自动开始
        st["recording"] = True
        st["buf"] = bytearray()
        st["last"] = time.monotonic()
        print("[xz] 收到首帧音频，自动开始")
        await broadcast_dash({"type": "event", "msg": "开始说话（收到音频）"})
        await broadcast_mic_status()
    payload = xz_strip_header(st.get("version", 0), data)
    if not payload:
        return
    pcm = xz_decode_opus(st, payload)
    if not pcm:
        return
    st["buf"] += pcm
    st["last"] = time.monotonic()
    st["level"] = frame_level(pcm)


def submit_buffer(ws, st):
    """把累积的 PCM 缓冲整段提交一次识别（FJ-mic 与 xiaozhi 共用）。"""
    pcm = bytes(st["buf"])
    st["buf"] = bytearray()
    st["recording"] = False
    st["ending"] = False
    duration_ms = int(len(pcm) / SAMPLE_BYTES / SAMPLE_RATE * 1000)
    print(f"[asr] 提交 {duration_ms} ms 音频识别")
    if len(pcm) >= FRAME_BYTES:
        if SAVE_WAV:
            write_wav(pcm)
            if DENOISE:
                try:
                    den = denoise_audio(
                        np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0)
                    write_wav(float_to_pcm16(den), tag="denoised")
                except Exception:  # noqa: BLE001
                    pass
        asr_queue.put((ws, pcm, duration_ms))
    else:
        if LOOP is not None:
            asyncio.run_coroutine_threadsafe(broadcast_dash({
                "type": "result", "text": "", "duration_ms": duration_ms,
                "asr_ms": 0, "ts": time.strftime("%H:%M:%S"), "wave": [],
            }), LOOP)
    if LOOP is not None:
        asyncio.run_coroutine_threadsafe(broadcast_mic_status(), LOOP)


# [遗留] 旧的独立监测台 HTTP 服务（:8080）。已被控制服务的 :8000/voice 取代，
# main() 不再调用本函数；保留仅供兼容参考，请勿在新流程中使用。
# ---------------------------------------------------------------------------
# HTTP：监测 GUI（index.html）
# ---------------------------------------------------------------------------
def serve_http():
    import http.server
    import os
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    http.server.HTTPServer(("0.0.0.0", HTTP_PORT),
                           http.server.SimpleHTTPRequestHandler).serve_forever()


# ===========================================================================
# 设备控制 / OTA 服务端（HTTP 局域网轮询协议 v1，端口默认 8000）
# 与 ASR(WebSocket 8765) 完全独立：独立线程 + 独立路由 + 独立状态。
# 语音监测台(index.html) 改由本服务在 /voice 统一托管（不再单独起 :8080）。
# 协议：ESP32 主动轮询 GET /api/v1/devices/{id}/commands/next；
#       PC 管理端 POST /api/v1/devices/{id}/commands 下发指令；
#       固件 GET /firmware/esp32s3/{version}/app.bin。
# 指令类型：servo_action / action_group / stop / reboot / ota。
# OTA 状态机：pending -> accepted -> downloading -> verifying -> rebooting
#            -> succeeded / failed / rollback。
# ===========================================================================
CTRL_ENABLED = True
CTRL_ONLY = False
CTRL_HOST = "0.0.0.0"
CTRL_PORT = 8000
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FIRMWARE_DIR = os.path.join(_SCRIPT_DIR, "firmware")
CTRL_HTML = os.path.join(_SCRIPT_DIR, "ctrl.html")
INDEX_HTML = os.path.join(_SCRIPT_DIR, "index.html")     # 语音监测台（原 :8080 监测 GUI）
PORTAL_HTML = os.path.join(_SCRIPT_DIR, "portal.html")   # 聚合首页：/ 入口
BRAIN_HTML = os.path.join(_SCRIPT_DIR, "brain.html")     # 大模型决策调试台
TWIN_HTML = os.path.join(_SCRIPT_DIR, "twin.html")       # 数字孪生主控台（从板端搬来）
DEVICE_TOKENS_FILE = None
CTRL_DEV_TOKEN = "dev-token-change-me"   # 开发默认 token；生产请改用 --device-tokens 配置

# ---------------------------------------------------------------------------
# [C-1] 状态存储（模块级全局 + 锁）
# DEVICES   设备注册表（心跳登记）  CMD_QUEUES 每设备唯一未完成指令
# FIRMWARE  固件索引                DEVICE_TOKENS 设备 token 表
# ---------------------------------------------------------------------------
DEVICES = {}          # device_id -> {fw_version, ip, rssi, free_heap, psram_free, busy, running_slot, last_heartbeat, last_status}
CMD_QUEUES = {}       # device_id -> 当前唯一指令(dict) 或 None（一设备同时仅一个未完成指令）
FIRMWARE = {}         # version -> {device, version, path, size, sha256}
DEVICE_TOKENS = {}    # device_id -> token
CTRL_LOCK = threading.Lock()

# ===========================================================================
# [C-6] 每设备状态机 + 会话注册表 —— 所有下行的调度中枢
#   状态由板子周期上报（平时走 WS 文本帧，OTA 阶段走 HTTP，因为那时 WS 已停）。
#   服务端据此做：录音/播放互斥、舵机优先级抢占、busy 拒绝、OTA 全量门禁。
# ===========================================================================
DEV_STATES = ("IDLE", "LISTENING", "SPEAKING", "MOVING", "UPGRADING", "ERROR", "OFFLINE")
STATE_TTL_S = 15.0          # 超过该秒数未上报 -> 判定 OFFLINE（仅在 WS 已断时才生效）
ACTION_TTL_S = 30.0         # 单个动作占用上限；超时未见板子回报完成则自动释放，避免永久锁死
OTA_TTL_S = 600.0           # OTA 会话上限（10 分钟）；超时未见终态则判 failed，避免永久门禁
# 板端 display_face.c 的 s_face_names 全表，字面必须完全一致（拼错板子直接 400）
FACES = ("idle", "listening", "thinking", "speaking",
         "happy", "angry", "sad", "joy", "error", "sleep")
FACE_DEFAULT_MS = 5000      # 表情默认持续 5s，播完自动回状态显示

# 板端可在 hello 或 {"type":"state"} 上报里顺带写的运行数据（有就记，没有就显示 —）
METRIC_FIELDS = ("fw_version", "free_heap", "psram_free", "rssi", "uptime_sec",
                 "running_slot", "ip")
# 板端同一项在不同报文里叫法不一样（device.status 里是 free_psram、free_internal），
# 这里给每个指标列别名，取到后统一按 METRIC_FIELDS 的名字入库，前端只认一套名字。
METRIC_ALIASES = {
    "psram_free": ("free_psram",),
    "free_heap": ("free_internal",),
    "rssi": ("wifi_rssi",),
    "uptime_sec": ("uptime", "uptime_s", "uptime_ms"),
}
# 板端 device.status 里的健康类布尔/计数，顺手收下来，供 UI 或排查用
METRIC_FLAGS = ("wifi_connected", "servo_busy", "sequence_playing", "audio_ws_ready",
                "audio_capture", "audio_playback", "playback_buffer_bytes",
                "playback_dropped_bytes")


def pick_metrics(src):
    """从板端上报（hello / state / device.status）里挑出运行数据并归一字段名。

    板端字段名不统一：device.status 回的是 free_psram / free_internal，而 UI 认的是
    psram_free / free_heap。在这层做别名映射，板端保持自己现有命名即可。
    """
    if not isinstance(src, dict):
        return {}
    out = {}
    for k in METRIC_FIELDS:
        for name in (k,) + tuple(METRIC_ALIASES.get(k) or ()):
            v = src.get(name)
            if v is not None:
                out[k] = v
                break
    for k in METRIC_FLAGS:
        v = src.get(k)
        if v is not None:
            out[k] = v
    return out

DEV_STATE = {}      # device_id -> {state, since, seq, last_seen, ip, session_id}
DEV_WS = {}         # device_id -> ws（音频下行 socket，hello 时按 device_id 绑定）
WS_DEVICE = {}      # ws -> device_id（断连时反查，用于解绑）
DEV_CURRENT = {}    # device_id -> {command, priority, since, protect} 当前舵机动作
PLAY_QUEUE = {}     # device_id -> [ {id, text} ] 待播队列
PLAY_CURRENT = {}   # device_id -> {id, abort, thread} 正在播放
OTA_SESSION = {}    # device_id -> {phase, version, since}
DEV_LOCK = threading.RLock()

# 命令优先级（服务端侧门禁）：数字越小越优先。
# 对照板端协议文档：policy=interrupt 的（停止类/单步类）排 P0~P2；policy=queue 的排 P3。
SERVO_PRIORITY = {
    "motion.emergency_stop": 0,     # 急停：最高优先级，policy=interrupt
    "action.stop": 1,               # 停止动作组：interrupt
    "sequence.stop": 1,             # 停止序列：interrupt
    "audio.play.stop": 1,           # 停止播放：interrupt
    "audio.capture.stop": 1,        # 停止录音：interrupt
    "device.reboot": 1,             # 重启：interrupt（且必须 async，重启后收不到回执）
    "servo.move": 2,                # 舵机移动：interrupt
    "servo.unload": 2,              # 舵机卸力：interrupt
    "audio.capture.start": 2,       # 开始录音：interrupt
    "action.run": 3,                # 动作组：queue
    "action.speed": 3,              # 动作速度：queue
    "sequence.run": 3,              # 长动作序列：queue
    "display.face": 9,              # 表情：可与舵机并行，不参与 busy 判定
}
PROTECTED_COMMANDS = {"sequence.run"}   # 长动作序列：仅 P0/P1 可打断，不被 P2/P3 覆盖

# 只读查询命令：不占用设备（不写 DEV_CURRENT / 不置 MOVING），也不受 busy 门禁限制
READONLY_COMMANDS = {"device.status", "servo.position.read", "servo.battery.read"}

# 板端协议明确要求的默认 policy（优先于按优先级推导）
COMMAND_POLICY = {
    "motion.emergency_stop": "interrupt",
    "action.stop": "interrupt",
    "sequence.stop": "interrupt",
    "audio.play.stop": "interrupt",
    "audio.capture.start": "interrupt",
    "audio.capture.stop": "interrupt",
    "servo.move": "interrupt",
    "servo.unload": "interrupt",
    "sequence.run": "queue",
    "display.face": "queue",
}

# 默认 mode：这些命令板端不会/不适合回 command_result，用 async 只确认送达
COMMAND_MODE = {
    "device.reboot": "async",       # 重启后连接即断开，sync 必定超时
    "sequence.run": "async",        # 协议文档示例为 async
    "action.run": "async",
    "display.face": "async",        # 协议文档示例为 async
}

TTS_HOOK = None     # 可插拔 TTS：def hook(text) -> 16k/单声道/int16 PCM 字节
TTS_NAME = "提示音占位（未接 TTS）"   # 供 /api/v1/info 与控制台显示当前音源


def _broadcast_state(device_id):
    """把设备状态变化推给监测台（从任意线程安全地投递到 asyncio 循环）。"""
    if not LOOP:
        return
    try:
        asyncio.run_coroutine_threadsafe(
            broadcast_dash({"type": "dev_state", "device_id": device_id,
                            "state": dev_get_state(device_id)}), LOOP)
    except Exception:  # noqa: BLE001
        pass


def _dev_metrics_sync(device_id, ip=None, metrics=None):
    """把 WS 侧上报的运行数据同步进旧注册表 DEVICES（任一上行里带了就算有值）。
    这样控制台 /api/v1/devices 那张表不用等 HTTP 心跳也能显示真实数值。"""
    try:
        with CTRL_LOCK:
            d = DEVICES.setdefault(device_id, {})
            if ip:
                d["ip"] = ip
            for k in METRIC_FIELDS + METRIC_FLAGS:
                if metrics and metrics.get(k) is not None:
                    d[k] = metrics[k]
            d["last_heartbeat"] = _ctrl_now_iso()
    except Exception:  # noqa: BLE001
        pass


def dev_set_state(device_id, state, ip=None, session_id=None, metrics=None):
    """更新设备状态；metrics 携带板端运行数据（固件/内存/信号/uptime 等）。"""
    if state not in DEV_STATES:
        return False
    with DEV_LOCK:
        rec = DEV_STATE.get(device_id) or {}
        changed = rec.get("state") != state
        rec.update({"state": state, "since": time.time(), "last_seen": time.time()})
        if ip:
            rec["ip"] = ip
        if session_id:
            rec["session_id"] = session_id
        if metrics:
            m = dict(rec.get("metrics") or {})
            m.update({k: v for k, v in metrics.items() if v is not None})
            rec["metrics"] = m
        rec["seq"] = int(rec.get("seq", 0)) + 1
        DEV_STATE[device_id] = rec
        # 板子回到空闲/错误/离线：释放动作占用，否则后续 P3 命令会被永久判 busy（死锁）
        if state in ("IDLE", "ERROR", "OFFLINE"):
            DEV_CURRENT.pop(device_id, None)
    # 周期上报带上板端运行数据：同步到旧注册表，让控制台那些列有真实数值
    if metrics or ip:
        _dev_metrics_sync(device_id, ip=ip, metrics=metrics)
    if changed:
        print(f"[dev] {device_id} -> {state}", flush=True)
        _broadcast_state(device_id)
    return changed


def _ws_alive(ws):
    """判断 WebSocket 连接是否仍然活着（兼容新旧 websockets 库）。"""
    if ws is None:
        return False
    try:
        closed = getattr(ws, "closed", None)
        if isinstance(closed, bool):
            return not closed
        stt = getattr(ws, "state", None)
        if stt is not None:
            name = str(getattr(stt, "name", stt)).upper()
            return name in ("OPEN", "CONNECTING")
        return True          # 判断不了就当活着，避免误杀
    except Exception:  # noqa: BLE001
        return True


def dev_get_state(device_id):
    """读状态，带 TTL 过期判定。

    关键：TTL 只在**没有活着的 WS 连接**时才判 OFFLINE。
    只要 socket 还在，即便板子长时间没上报（例如无周期心跳、只在事件时上报），
    也只标记 stale=True，不能把在线设备误判离线而拒绝下发命令
    （UPGRADING 不判过期，升级本身可能很慢）。
    """
    with DEV_LOCK:
        rec = DEV_STATE.get(device_id)
        ws = DEV_WS.get(device_id)
    if not rec:
        return None
    alive = _ws_alive(ws)
    age = time.time() - rec.get("last_seen", 0)
    out = dict(rec)
    out["age"] = round(age, 1)
    out["ws_alive"] = alive
    expired = age > STATE_TTL_S and rec.get("state") != "UPGRADING"
    if expired and not alive:
        out["state"] = "OFFLINE"
        out["stale"] = True
    else:
        out["stale"] = bool(expired)   # 连着但状态陈旧
    return out


def _release_stale_ota(device_id):
    """兜底：OTA 会话长时间既没有推进也没有终态（板子中途死机/断网），
    自动判失败。否则 OTA 门禁会永久拒绝该设备的一切命令。"""
    with DEV_LOCK:
        ota = OTA_SESSION.get(device_id)
        if not ota:
            return False
        if ota.get("phase") in ("succeeded", "failed", "rollback", "interrupted"):
            return False
        if time.time() - ota.get("since", 0) <= OTA_TTL_S:
            return False
        ota = dict(ota)
        ota.update({"phase": "failed", "since": time.time(),
                    "error": f"OTA 超时未回报终态（>{int(OTA_TTL_S)}s）"})
        OTA_SESSION[device_id] = ota
        print(f"[ota] {device_id} 超时未回报终态，自动判失败", flush=True)
        return True


def dev_state_all():
    # 每次对外输出前先做一次兜底清理，保证 UI 上看到的不是残留的旧数据
    with DEV_LOCK:
        ids = sorted(set(list(DEV_STATE) + list(DEV_WS) + list(DEVICE_ENDPOINTS)))
    for d in ids:
        try:
            _release_stale_action(d)
            _release_stale_ota(d)
        except Exception:  # noqa: BLE001
            pass
    return {d: (dev_get_state(d) or _unreported(d)) for d in ids}


def _unreported(device_id):
    """只在端点表里登记过、从未上报过状态的条目（如手动登记的 IP）。"""
    ep = DEVICE_ENDPOINTS.get(device_id) or {}
    out = {"state": "OFFLINE", "note": "从未上报", "age": None}
    if ep:
        out["ip"] = ep.get("host")
        out["http_port"] = ep.get("port")
        out["source"] = "auto(WS)" if ep.get("auto") else "manual"
    return out


def dev_bind_ws(device_id, ws, session_id=None, ip=None):
    with DEV_LOCK:
        DEV_WS[device_id] = ws
    if session_id or ip:
        with DEV_LOCK:
            rec = DEV_STATE.get(device_id) or {}
            if session_id:
                rec["session_id"] = session_id
            if ip:
                rec["ip"] = ip
            rec["last_seen"] = time.time()
            DEV_STATE[device_id] = rec
    print(f"[dev] {device_id} 已绑定音频 WS", flush=True)


def dev_unbind_ws(device_id, ws=None):
    """WS 断连：解绑下行目标，并清理该设备所有运行时占用。

    以前只把 state 改成 OFFLINE，导致断开后残留：
      DEV_CURRENT（界面一直显示旧动作、且重连后 gate 误判 busy）
      PLAY_QUEUE / PLAY_CURRENT（播放线程可能还在跑）
      OTA_SESSION（卡在 downloading，永久性 OTA 门禁，拒绝一切命令）
    """
    if not device_id:
        return
    with DEV_LOCK:
        if ws is None or DEV_WS.get(device_id) is ws:
            DEV_WS.pop(device_id, None)
        rec = DEV_STATE.get(device_id)
        if rec:
            rec["state"] = "OFFLINE"
            rec["ws_alive"] = False
            rec["last_seen"] = rec.get("last_seen", time.time())
            DEV_STATE[device_id] = rec
        cur = DEV_CURRENT.pop(device_id, None)
        queue = PLAY_QUEUE.pop(device_id, None)
        playing = PLAY_CURRENT.get(device_id)
        ota = OTA_SESSION.get(device_id)
        if ota and ota.get("phase") not in ("succeeded", "failed", "rollback", "interrupted"):
            ota = dict(ota)
            ota.update({"phase": "interrupted", "since": time.time(),
                        "error": "WebSocket 断连"})
            OTA_SESSION[device_id] = ota
    # 播放线程置 abort，让它尽快自行退出（不放锁，避免死锁）
    if playing:
        try:
            playing["abort"] = True
            playing["aborted"] = True
        except Exception:  # noqa: BLE001
            pass
    if cur or queue or playing:
        print(f"[dev] {device_id} 断连清理：占用={cur and cur.get('command')} "
              f"队列={len(queue or [])} 播中={bool(playing)}", flush=True)
    # 孪生后台线程（步态/动作组循环）也要停：板子都断了，再空转没有意义，
    # 而且会给一台离线设备继续刷 servo.move，日志和控制台都不好看。
    try:
        if TWIN_GAIT is not None or TWIN_SEQ is not None:
            twin_stop_all(device_id, reason="ws disconnected")
    except Exception as e:  # noqa: BLE001
        print(f"[twin] 断连停步态失败：{e}", flush=True)


def dev_get_ws(device_id):
    with DEV_LOCK:
        return DEV_WS.get(device_id)


def _release_stale_action(device_id):
    """兜底：动作占用超时未见板子回报完成，自动释放，避免服务端把设备永久锁在 busy。

    TTL 优先取该命令自己的预计时长（hold_ms）：servo.move 带 time=300 就只占 0.6s，
    不必干等 30s。板端目前不发周期 state 上报，"板子回 IDLE 才释放"这条路走不通，
    所以瞬时命令必须靠自己的时长自我了断——否则手动下发一次舵机，30 秒内谁都动不了。
    """
    with DEV_LOCK:
        cur = DEV_CURRENT.get(device_id)
        ttl = None
        if cur:
            hold = cur.get("hold_ms")
            ttl = (hold / 1000.0) if hold is not None else ACTION_TTL_S
        if cur and time.time() - cur.get("since", 0) > ttl:
            DEV_CURRENT.pop(device_id, None)
            print(f"[dev] {device_id} 动作占用超时未回报({cur.get('command')}, "
                  f"{cur.get('hold_ms') or int(ACTION_TTL_S * 1000)}ms)，自动释放",
                  flush=True)
            momentary = cur.get("command") in ("servo.move", "servo.unload")
            cmd = cur.get("command")
        else:
            momentary, cmd = False, None
    # 瞬时舵机命令跑完了就该回到 IDLE；长命令/序列的状态交给板子上报决定
    if momentary:
        st = dev_get_state(device_id) or {}
        if st.get("state") == "MOVING":
            dev_set_state(device_id, "IDLE")
    return bool(cmd)


def dev_gate(device_id, command, policy=None):
    """门禁：返回 (ok, reason, hint)。所有下行命令都先过这里。

    policy="interrupt" 表示这条命令就是要抢占板子的当前动作（大脑层下的动作都走这条），
    此时不按 busy 拒绝——否则主人连着说两个动作，第二条会被自家门禁挡掉。
    """
    st = dev_get_state(device_id) or {}
    cur_state = st.get("state", "OFFLINE")
    prio = SERVO_PRIORITY.get(command, 3)
    if cur_state == "UPGRADING":
        return False, "upgrading", "设备正在 OTA 升级，拒绝一切命令"
    if cur_state == "OFFLINE" and st.get("stale") and not st.get("ws_alive"):
        return False, "offline", "WebSocket 已断开且长时间未上报状态，可能已离线"
    # 只读查询 / 设备级命令（status、reboot、read）：不占设备，不受 busy 限制
    if command in READONLY_COMMANDS or command.startswith("device."):
        return True, "ok", ""
    # 表情与媒体类：可与舵机动作并行，只受 OTA 门禁 + 录音/播放互斥约束
    if command == "display.face" or command.startswith("audio."):
        if cur_state == "LISTENING" and command.startswith("audio.play"):
            return False, "busy", "正在录音，禁止同时播放（板端会拒 INVALID_STATE）"
        if cur_state == "SPEAKING" and command.startswith("audio.capture"):
            return False, "busy", "正在播放，禁止同时录音（避免自触发）"
        return True, "ok", ""
    if _release_stale_action(device_id):
        dev_set_state(device_id, "IDLE")
        cur_state = "IDLE"
    with DEV_LOCK:
        cur = DEV_CURRENT.get(device_id)
    if cur and cur.get("protect") and prio >= 2:
        return False, "busy", f"长动作序列执行中（{cur.get('command')}），仅急停/停止可打断"
    if cur and prio >= 3 and policy != "interrupt":
        return False, "busy", f"设备忙（{cur.get('command')}），请排队或稍后重试"
    if cur_state == "LISTENING" and command.startswith("audio.play"):
        return False, "busy", "正在录音，禁止同时播放（避免自触发）"
    if cur_state == "SPEAKING" and command.startswith("audio.capture"):
        return False, "busy", "正在播放，禁止同时录音（避免自触发）"
    return True, "ok", ""


# ---------- WS 控制指令层（板端统一 JSON 命令入口）----------
CMD_PENDING = {}    # request_id -> {"event": Event, "result": dict}
CMD_SENT = {}       # request_id -> {device_id, command, t}：async 指令的发出记录。
                    # async 不等回执，但板子照样会回 command_result；留着这条，
                    # 迟到的回执才能对上号（判失败 + 记账），顺带清掉过期条目。
CMD_SENT_TTL_S = 60.0


def _cmd_sent_gc():
    """清掉过期的 async 发送记录（WS 断连/板子不回时不会无限堆积）。"""
    now = time.time()
    with DEV_LOCK:
        for rid in [r for r, v in CMD_SENT.items() if now - v.get("t", 0) > CMD_SENT_TTL_S]:
            CMD_SENT.pop(rid, None)


def ws_on_command_result(j):
    """板子回执：{"type":"command_result","request_id":...,"ok":...,"error":{...}}

    sync 指令：唤醒等待方。async 指令没有等待方，靠 CMD_SENT 对上号并记一次账——
    否则板子回的 400/409 会被静默丢掉，面板上完全看不出"指令其实被拒了"。
    """
    rid = j.get("request_id")
    if not rid:
        return
    with DEV_LOCK:
        p = CMD_PENDING.get(rid)
    if p:
        p["result"] = j
        p["event"].set()
        print(f"[ws<-] command_result rid={rid} ok={j.get('ok')} "
              f"status={j.get('status') or (j.get('data') or {}).get('status')} "
              f"error={j.get('error')}", flush=True)
    else:
        with DEV_LOCK:
            rec = CMD_SENT.pop(rid, None)
        # quiet 的指令（步态帧）成功时不打日志：8Hz × 全天会把日志冲爆。
        # 失败仍然要打——板端拒了命令却一声不吭，才是最难受的。
        if j.get("ok") and rec and rec.get("quiet"):
            return
        who = f" device={rec.get('device_id')} cmd={rec.get('command')}" if rec else ""
        print(f"[ws<-] command_result rid={rid}{who}（async，无等待方）"
              f"ok={j.get('ok')}", flush=True)
    if not j.get("ok"):
        err = j.get("error") or {}
        print(f"[cmd] 回执失败 rid={rid} code={err.get('code')} "
              f"message={err.get('message')}", flush=True)


def ws_command(device_id, command, args=None, mode="async", policy="queue", timeout=5.0,
               quiet=False):
    """通过已绑定的音频 WS 下发一条 JSON 控制指令。
    mode=async 只确认送达；mode=sync 等板子的 command_result 回执。
    quiet=True 不打逐帧日志（步态 8Hz 下发，逐帧打会把日志冲爆）；失败照打。"""
    ws = dev_get_ws(device_id)
    if ws is None:
        return {"ok": False, "error": "设备离线：无音频 WS 连接（板子需先连 ?role=dog 并发 hello）"}
    rid = _ctrl_gen_id()
    env = {"type": "command", "version": 1, "request_id": rid,
           "command": command, "mode": mode, "policy": policy, "args": args or {}}
    ev = threading.Event()
    with DEV_LOCK:
        CMD_PENDING[rid] = {"event": ev, "result": None}
    payload = json.dumps(env, ensure_ascii=False)
    if not _ws_call(lambda: ws.send(payload)):
        with DEV_LOCK:
            CMD_PENDING.pop(rid, None)
        print(f"[ws->] 发送失败 device={device_id} {command} rid={rid}", flush=True)
        return {"ok": False, "error": "ws send failed", "request_id": rid}
    if not quiet:
        print(f"[ws->] {device_id} <- {command} rid={rid} mode={mode} policy={policy} "
              f"args={json.dumps(args or {}, ensure_ascii=False)}", flush=True)
    if mode != "sync":
        # async 不等回执：立刻注销等待方（否则迟到的回执只写不删，字典会越攒越大），
        # 改为在 CMD_SENT 记一笔，让板子稍后回的结果还能对上号、能判失败。
        with DEV_LOCK:
            CMD_PENDING.pop(rid, None)
            CMD_SENT[rid] = {"device_id": device_id, "command": command,
                             "t": time.time(), "quiet": quiet}
        _cmd_sent_gc()
        return {"ok": True, "request_id": rid, "sent": env, "note": "async 已送达"}
    if not ev.wait(timeout):
        with DEV_LOCK:
            CMD_PENDING.pop(rid, None)
        return {"ok": False, "error": f"timeout: 板子未在 {timeout}s 内回 command_result",
                "request_id": rid}
    with DEV_LOCK:
        res = CMD_PENDING.pop(rid, {}).get("result") or {}
    okb = bool(res.get("ok"))
    out = {"ok": okb, "request_id": rid, "result": res}
    if not okb:
        # 把板端的 error.code/message 抬到顶层：上层（大脑层补话、调试台）才能说清"为什么没做成"
        err = res.get("error") or {}
        code = err.get("code") or "ERROR"
        out["error"] = f"板端拒绝 {code}：{err.get('message') or ''}".strip("： ")
        out["error_code"] = code
        out["retryable"] = err.get("retryable")
    return out


# ---------- 优先级调度（服务端门禁 + 板端 policy）----------
def _command_hold_ms(command, args):
    """这条命令预计要占设备多久（毫秒）。None = 用 ACTION_TTL_S 兜底。

    板端不发周期 state 上报、也没有动作完成事件，所以服务端只能自己估：
    估短了会让长动作被误判为空闲，估长了会让设备白白锁住。宁可略长一点。
    """
    args = args or {}
    if command == "servo.move":
        servos = args.get("servos") or []
        slowest = 0
        for s in servos:
            try:
                slowest = max(slowest, int(s.get("time") or 0))
            except (TypeError, ValueError):
                continue
        return slowest + 300
    if command == "servo.unload":
        return 300
    if command == "sequence.run" and twin_mod is not None:
        steps = args.get("steps") or []
        if steps:
            return twin_mod.sequence_duration_ms(steps) + 500
    if command == "action.run":
        return None            # 预存动作组时长未知，用默认 TTL
    return None


def servo_dispatch(device_id, command, args=None, priority=None, policy=None,
                   protect=False, mode=None):
    """优先级调度：服务端门禁 -> 抢占/保护 -> WS 下发。

    policy 取板端协议默认值 COMMAND_POLICY，缺省时按优先级推导（P0~P2 interrupt，其余 queue）。
    mode 取 COMMAND_MODE，缺省 sync。只读命令不占用设备（不写 DEV_CURRENT / 不置 MOVING）。
    """
    prio = int(priority) if priority is not None else SERVO_PRIORITY.get(command, 3)
    pol = policy or COMMAND_POLICY.get(command) or ("interrupt" if prio <= 2 else "queue")
    md = mode or COMMAND_MODE.get(command, "sync")
    ok, reason, hint = dev_gate(device_id, command, policy=pol)
    if not ok:
        return {"ok": False, "error": reason, "hint": hint, "priority": prio}
    # 只读查询 / 设备级 / 表情：不占用设备，不置 MOVING，不会阻塞后续舵机命令
    readonly = (command in READONLY_COMMANDS
                or command.startswith("device.")
                or command == "display.face")
    if prio <= 1:
        play_abort(device_id)          # 急停/停止：同时打断语音播放
        with DEV_LOCK:
            DEV_CURRENT.pop(device_id, None)
        dev_set_state(device_id, "IDLE")
    elif not readonly:
        with DEV_LOCK:
            DEV_CURRENT[device_id] = {"command": command, "priority": prio,
                                      "since": time.time(),
                                      "hold_ms": _command_hold_ms(command, args),
                                      "protect": protect or command in PROTECTED_COMMANDS}
        dev_set_state(device_id, "MOVING")
    res = ws_command(device_id, command, args or {}, mode=md, policy=pol)
    res["priority"] = prio
    res["policy"] = pol
    res["mode"] = md
    return res


# ---------------------------------------------------------------------------
# 数字孪生（从板端页面搬到服务端）
#
# 原来这层整个活在 main/web_page.h 内嵌的浏览器 JS 里：标定表和动作组存在浏览器
# 本地存储（换机器即丢、大模型读不到），步态跑 requestAnimationFrame（标签页转后台
# 就被降到 1Hz），页面直连板端 /api/* 还绕过了这里的鉴权与调度。
# 现在数据落盘成 twin_store.json，步态变成后台线程，下发统一走上面的 WS 命令层。
# ---------------------------------------------------------------------------
try:
    import twin as twin_mod
except Exception as _e:  # noqa: BLE001
    twin_mod = None
    print(f"[twin] twin.py 加载失败，孪生功能不可用：{_e}", flush=True)

TWIN_GAIT = None
TWIN_SEQ = None


def _twin_gait_dispatch(device_id, servos, move_ms=None):
    """步态单帧下发：直接走 WS，绕开 dev_gate。

    步态自己就是设备当前的占用者（DEV_CURRENT 里标着 twin.gait），再走一遍门禁
    会被自家的 busy 判据挡下。顺便把 since 往前推，免得被 30s 的看门狗当残留清掉。
    """
    with DEV_LOCK:
        cur = DEV_CURRENT.get(device_id)
        if cur and cur.get("command") == "twin.gait":
            cur["since"] = time.time()
    return ws_command(device_id, "servo.move", {"servos": servos},
                      mode="async", policy="interrupt", quiet=True)


def _twin_seq_dispatch(device_id, steps):
    with DEV_LOCK:
        cur = DEV_CURRENT.get(device_id)
        if cur and cur.get("command") == "twin.sequence":
            cur["since"] = time.time()
    return ws_command(device_id, "sequence.run", {"steps": steps},
                      mode="async", policy="queue", quiet=True)


def _twin_seq_on_start(device_id):
    with DEV_LOCK:
        DEV_CURRENT[device_id] = {"command": "twin.sequence", "priority": 3,
                                  "since": time.time(), "protect": True}
    dev_set_state(device_id, "MOVING")


def _twin_seq_on_stop(device_id, reason="user"):
    if not device_id:
        return
    with DEV_LOCK:
        cur = DEV_CURRENT.get(device_id)
        if cur and cur.get("command") == "twin.sequence":
            DEV_CURRENT.pop(device_id, None)
    dev_set_state(device_id, "IDLE")


def _twin_gait_on_start(device_id):
    with DEV_LOCK:
        DEV_CURRENT[device_id] = {"command": "twin.gait", "priority": 2,
                                  "since": time.time(), "protect": True}
    dev_set_state(device_id, "MOVING")


def _twin_gait_on_stop(device_id, reason="user"):
    if not device_id:
        return
    with DEV_LOCK:
        cur = DEV_CURRENT.get(device_id)
        if cur and cur.get("command") == "twin.gait":
            DEV_CURRENT.pop(device_id, None)
    dev_set_state(device_id, "IDLE")


if twin_mod is not None:
    TWIN_GAIT = twin_mod.GaitRunner(twin_mod.load, _twin_gait_dispatch,
                                    on_start=_twin_gait_on_start,
                                    on_stop=_twin_gait_on_stop)
    TWIN_SEQ = twin_mod.SequenceLooper(_twin_seq_dispatch,
                                       on_start=_twin_seq_on_start,
                                       on_stop=_twin_seq_on_stop)


def twin_available():
    return twin_mod is not None and TWIN_GAIT is not None


def twin_stop_all(device_id=None, reason="shutdown"):
    """停掉该设备（或全部）孪生后台线程。断连/关机时调用。"""
    if TWIN_GAIT is not None and TWIN_GAIT.is_running(device_id):
        TWIN_GAIT.stop(reason=reason)
    if TWIN_SEQ is not None and TWIN_SEQ.is_running(device_id):
        TWIN_SEQ.stop()


def twin_pick_device(explicit=None):
    """选孪生要控制的设备：显式指定 > 唯一在线的 WS 设备 > DOG_BRAIN_DEVICE。"""
    if explicit and str(explicit).strip():
        return str(explicit).strip()
    with DEV_LOCK:
        online = sorted(DEV_WS.keys())
    if len(online) == 1:
        return online[0]
    env = (os.environ.get("DOG_BRAIN_DEVICE") or "").strip()
    if env:
        return env
    return online[0] if online else ""


def twin_devices():
    """孪生页面顶栏的设备下拉：列出所有知道的设备 + 是否在线。"""
    with DEV_LOCK:
        ids = sorted(set(list(DEV_WS.keys()) + list(DEV_STATE.keys())
                         + list(DEVICE_ENDPOINTS.keys())))
        ws_ids = set(DEV_WS.keys())
    out = []
    for did in ids:
        st = dev_get_state(did) or {}
        out.append({"device_id": did, "online": did in ws_ids,
                    "state": st.get("state", "OFFLINE"),
                    "ip": st.get("ip") or (DEVICE_ENDPOINTS.get(did) or {}).get("host")})
    return out


def twin_positions(device_id, ids):
    """读舵机真实位置：走 WS 命令层（板端 servo.position.read）。"""
    res = servo_dispatch(device_id, "servo.position.read", {"ids": list(ids)})
    if not res.get("ok"):
        return res
    payload = (res.get("result") or {}).get("result") or {}
    res["positions"] = payload.get("positions") or []
    return res


def twin_battery(device_id):
    res = servo_dispatch(device_id, "servo.battery.read", {})
    if not res.get("ok"):
        return res
    payload = (res.get("result") or {}).get("result") or {}
    res["voltage_mv"] = payload.get("voltage_mv")
    return res


def twin_can_take_over(device_id, self_cmd):
    """孪生要独占设备（起步态 / 起动作组循环）时的前置检查。

    不能拿 dev_gate("sequence.run") 当代理判据：那条路会被任何一条瞬时命令
    （刚下发的一次 servo.move）判成 busy，于是"刚调完一个舵机就点开始 trot"
    会被自家门禁拒掉。这里只在意真正会长时间霸占设备的东西。

    self_cmd 是本次要起的占用名：别人的占用要拒，自己重入交给 runner 自己判。
    """
    st = dev_get_state(device_id) or {}
    if st.get("state") == "UPGRADING":
        return False, "upgrading", "设备正在 OTA 升级，拒绝一切命令"
    if dev_get_ws(device_id) is None:
        return False, "offline", "设备离线：没有音频 WS 连接"
    _release_stale_action(device_id)
    with DEV_LOCK:
        cur = DEV_CURRENT.get(device_id)
    if cur and cur.get("protect") and cur.get("command") != self_cmd:
        return False, "busy", f"设备正被 {cur.get('command')} 占用，先停掉它"
    return True, "ok", ""


def twin_status_payload():
    """孪生台一次性拿全：设备列表、目标设备、步态/循环状态、store 元信息。"""
    store = twin_mod.load() if twin_mod else {}
    target = twin_pick_device()
    # 查询前先兜底清理：瞬时命令的占用是按自身时长过期的，gate 只在下发路径上清理，
    # 不在这里补一次的话，面板会一直显示"设备忙"直到下一次下发（和 dev_state_all 同理）
    _release_stale_action(target)
    st = dev_get_state(target) or {}
    with DEV_LOCK:
        cur = DEV_CURRENT.get(target)
    return {
        "ok": True,
        "devices": twin_devices(),
        "target": target,
        # 目标设备的调度占用与状态：页面据此提示"板子正忙/序列播放中"，
        # 不用去戳板子（250ms 戳一次会把板端日志和舵机总线都占满）
        "target_state": st.get("state", "OFFLINE"),
        "target_busy": bool(cur),
        "target_current": cur.get("command") if cur else None,
        "target_playing_sequence": bool(cur and cur.get("command") == "twin.sequence"),
        "gait": TWIN_GAIT.state() if TWIN_GAIT else {"running": False},
        "sequence": TWIN_SEQ.state() if TWIN_SEQ else {"running": False},
        "store_path": twin_mod.STORE_PATH if twin_mod else None,
        "store_updated_at": store.get("updated_at"),
        "layout": {"legs": [l["name"] for l in twin_mod.LEGS],
                   "dof": list(twin_mod.DOF),
                   "dof_label": list(twin_mod.DOF_LABEL),
                   "motion_group_count": twin_mod.MOTION_GROUP_COUNT,
                   "motion_max_steps": twin_mod.MOTION_MAX_STEPS},
    }

# ---------- WS 下行语音播放 ----------
def audio_make_packet(opus_bytes):
    """下行帧头：0x00 0x00 len_hi len_lo + Opus 载荷（与上行格式一致）。"""
    n = len(opus_bytes)
    return bytes([0x00, 0x00, (n >> 8) & 0xFF, n & 0xFF]) + opus_bytes


def _ws_call(factory, timeout=5):
    """从工作线程安全地把一帧投到 asyncio 的 WS 连接上。"""
    if LOOP is None:
        return False
    try:
        asyncio.run_coroutine_threadsafe(factory(), LOOP).result(timeout)
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[ws] 下行发送失败：{e}", flush=True)
        return False


def _ws_send_json(ws, obj):
    return _ws_call(lambda: ws.send(json.dumps(obj, ensure_ascii=False)))


def _beep_pcm():
    """占位音源：440Hz 提示音 0.6s。TTS 不可用/失败时用它保住播放链路。"""
    import math
    n = int(16000 * 0.6)
    buf = bytearray()
    for i in range(n):
        v = int(6000 * math.sin(2 * math.pi * 440 * i / 16000.0)
                * (0.5 + 0.5 * math.sin(2 * math.pi * 2 * i / 16000.0)))
        buf += struct.pack("<h", max(-32767, min(32767, v)))
    return bytes(buf)


def synth_speech(text):
    """可插拔音源：优先 TTS_HOOK，未接或失败则回退提示音。

    TTS_HOOK 契约：fn(text) -> 16000Hz / 单声道 / int16 裸 PCM（不带 WAV 头）。
    返回空或抛异常一律视作合成失败，回退提示音，避免整条播放链路断掉。
    """
    if TTS_HOOK is not None:
        try:
            pcm = TTS_HOOK(text) or b""
        except Exception as e:  # noqa: BLE001
            print(f"[tts] 合成失败，回退提示音：{e}", flush=True)
            pcm = b""
        if pcm:
            # 契约兜底：必须按 2 字节对齐，否则最后一帧会编码失败
            if len(pcm) % 2:
                pcm = pcm[: len(pcm) // 2 * 2]
            return pcm
    return _beep_pcm()


def _load_tts():
    """启动时尝试接入 tts_edge（edge-tts）；接不上就继续用提示音，不影响服务启动。"""
    global TTS_HOOK, TTS_NAME
    try:
        import tts_edge
        TTS_HOOK = tts_edge.synth
        TTS_NAME = f"edge-tts · {tts_edge.VOICE}"
        print(f"[tts] 已接入 {TTS_NAME}", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"[tts] 未接入 edge-tts（{e}），播放继续用提示音占位", flush=True)


# ---------------------------------------------------------------------------
# [C-7] 大脑层：ASR 文本 -> 云端大模型 -> 说话 + 表情 + 动作
# 默认关闭（设 DOG_BRAIN=1 开启），避免没配 Key 时干扰现有识别流程。
# ---------------------------------------------------------------------------
BRAIN = None
BRAIN_ENABLED = str(os.environ.get("DOG_BRAIN", "0")).lower() not in ("", "0", "false", "no")
BRAIN_INFO = {"enabled": BRAIN_ENABLED, "ready": False}
BRAIN_LOCK = threading.Lock()


def _device_id_by_ws(ws):
    """反查某条 WS 属于哪台设备（dog 角色绑定过 DEV_WS）。"""
    if ws is None:
        return None
    with DEV_LOCK:
        for did, w in DEV_WS.items():
            if w is ws:
                return did
    return None


def _brain_action(device_id, action_name):
    """动作执行回调：把能力清单里的动作名翻译成板端的 command envelope。

    板端只认 action.run 的 args.group（控制板预存动作组编号 0~255），**不认动作名**。
    所以清单里必须配 args；缺了就当场报错，绝不退回 {"name": ...}——
    那种信封板端必然判 INVALID_ARGUMENT，狗会"说了做不到"还浑然不觉。
    """
    if BRAIN is None:
        return {"ok": False, "error": "brain not loaded"}
    for a in BRAIN.caps.get("actions", []):
        if a.get("name") != action_name:
            continue
        cmd = a.get("cmd") or "action.run"
        args = dict(a.get("args") or {})
        if not args:
            return {"ok": False, "name": action_name,
                    "error": f"动作 {action_name} 没配板端参数",
                    "hint": '在 dog_actions.json 里补 args，例如 {"group":1,"times":1}'}
        if cmd == "action.run":
            if args.get("group") is None:
                return {"ok": False, "name": action_name,
                        "error": f"动作 {action_name} 缺 args.group",
                        "hint": "板端只认预存动作组编号，请填控制板里实际存的组号"}
            args.setdefault("times", 1)      # 板端 json_get_int_range 里 times 必填，缺了直接 400
        # 主人说话时下的动作应当直接打断板子正在跑的动作组；
        # 排队（queue）在板端 sequence 播放期间会被 409 拒掉，狗就只会说话不动
        policy = a.get("policy") or ("interrupt" if cmd == "action.run" else None)
        # 必须用 sync：板子会在毫秒级回 command_result，拿到真实成败才能触发"补话"；
        # 走 async 的话服务端永远只看到"已送达"，狗被 409 拒了还会嘴硬说做完了。
        return servo_dispatch(device_id, cmd, args, policy=policy, mode="sync")
    return {"ok": False, "error": "unknown action", "name": action_name}


def _load_brain():
    """启动时装配大脑层。缺 Key / 缺包都只是不启用，不影响服务本身。"""
    global BRAIN, BRAIN_INFO
    if not BRAIN_ENABLED:
        BRAIN_INFO = {"enabled": False, "ready": False,
                      "reason": "未开启：启动前设 DOG_BRAIN=1"}
        return
    try:
        import brain
        import llm_cloud
        BRAIN = brain.DogBrain(actors={"face": face_show,
                                       "action": _brain_action,
                                       "speak": audio_play_text})
        info = llm_cloud.describe()
        info.update({"enabled": True, "ready": info.get("ready", False),
                     "actions": BRAIN.action_names(),
                     "faces": BRAIN.caps.get("faces", [])})
        BRAIN_INFO = info
        if info.get("ready"):
            print(f"[brain] 已启用：{info['provider']} / {info['model']}，"
                  f"动作 {len(info['actions'])} 个", flush=True)
        else:
            print("[brain] 已装载但未配置 Key：设 LLM_PROVIDER / LLM_API_KEY 后重启", flush=True)
    except Exception as e:  # noqa: BLE001
        BRAIN = None
        BRAIN_INFO = {"enabled": True, "ready": False, "error": str(e)}
        print(f"[brain] 装载失败，对话功能不可用：{e}", flush=True)


def _brain_config_dict():
    """当前大模型配置（含掩码后的 Key，不回显原文）。"""
    import llm_cloud
    d = llm_cloud.describe()
    d["config_file"] = llm_cloud.config_path()
    return d


def _brain_write_config(body):
    """把配置写进 llm_config.json。llm_cloud 按 mtime 热读，写完下次调用即生效。

    api_key 传空串表示"不改"；换 provider 时若没给 model，就退回该平台的预设默认模型，
    免得拿 glm 的模型名去打 deepseek。
    """
    import llm_cloud
    path = llm_cloud.config_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f) or {}
    except Exception:  # noqa: BLE001
        cfg = {}
    prov = str(body.get("provider") or "").strip().lower()
    if prov:
        cfg["provider"] = prov
    model = str(body.get("model") or "").strip()
    if model:
        cfg["model"] = model
    elif prov and prov in llm_cloud.PRESETS and cfg.get("provider") != prov:
        cfg["model"] = llm_cloud.PRESETS[prov]["model"]
    key = str(body.get("api_key") or "").strip()
    if key:
        cfg["api_key"] = key
    base = str(body.get("base_url") or "").strip()
    if base:
        cfg["base_url"] = base
    elif prov and prov != "custom":
        cfg.pop("base_url", None)      # 交给平台预设，避免残留旧端点
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    return _brain_config_dict()


def _brain_ping():
    """连通自检：不带工具问一句，验证 Key / 端点 / 模型名。"""
    if BRAIN is None:
        return {"ok": False, "error": "brain 未启用（启动时设 DOG_BRAIN=1）"}
    return BRAIN.probe()


def _brain_dry_run(device_id, text):
    """预演：走完整决策链路但不下发任何动作/播报，用于调试台看模型到底给了什么。"""
    if BRAIN is None:
        return {"ok": False, "error": "brain 未启用（启动时设 DOG_BRAIN=1）"}
    return BRAIN.handle(device_id, text, dry=True)


def brain_say(device_id, text, wait=False):
    """把一句话交给大脑。默认后台线程执行，避免阻塞 ASR 队列。"""
    if BRAIN is None:
        return {"ok": False, "error": "brain 未启用（DOG_BRAIN=1 且配好 LLM_API_KEY）"}
    # 实时读配置：llm_config.json 改完立刻生效，不用重启服务
    try:
        import llm_cloud
        cfg = llm_cloud.describe()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"配置读取失败：{e}"}
    if not cfg.get("ready"):
        return {"ok": False,
                "error": "大模型未就绪：把 api_key 填进 server/llm_config.json（或设 LLM_API_KEY）",
                "config": {k: cfg.get(k) for k in ("provider", "base_url", "model", "key_set")},
                "config_file": llm_cloud.config_path()}
    if wait:
        return BRAIN.handle(device_id, text)
    threading.Thread(target=BRAIN.handle, args=(device_id, text),
                     daemon=True, name="brain").start()
    return {"ok": True, "async": True}


def audio_play_text(device_id, text):
    """入队一段待播语音；不忙则立即起播。入队前先过门禁，避免放行后才被拒。"""
    ok, reason, hint = dev_gate(device_id, "audio.play.start")
    if not ok:
        return {"ok": False, "error": reason, "hint": hint}
    item = {"id": _ctrl_gen_id(), "text": text}
    with DEV_LOCK:
        PLAY_QUEUE.setdefault(device_id, []).append(item)
        busy = device_id in PLAY_CURRENT
        pos = len(PLAY_QUEUE[device_id])
    if not busy:
        _play_next(device_id)
    return {"ok": True, "id": item["id"], "position": pos}


def _play_next(device_id):
    with DEV_LOCK:
        if device_id in PLAY_CURRENT:
            return
        q = PLAY_QUEUE.get(device_id) or []
        if not q:
            return
        item = q.pop(0)
        ev = threading.Event()
        th = threading.Thread(target=_play_worker, args=(device_id, item, ev), daemon=True)
        PLAY_CURRENT[device_id] = {"id": item["id"], "abort": ev, "thread": th}
    th.start()


def _play_worker(device_id, item, abort):
    ws = dev_get_ws(device_id)
    if ws is None:
        print(f"[play] {device_id} 无音频 WS 连接，丢弃一条播放", flush=True)
        with DEV_LOCK:
            PLAY_CURRENT.pop(device_id, None)
        return
    ok, reason, hint = dev_gate(device_id, "audio.play.start")
    if not ok:
        print(f"[play] 被门禁拒绝：{reason} {hint}", flush=True)
        with DEV_LOCK:
            PLAY_CURRENT.pop(device_id, None)
        return
    sid = (DEV_STATE.get(device_id) or {}).get("session_id") or ""
    sr, fd = 16000, 60
    samples = sr * fd // 1000
    # 先把音频合成出来再发 start：TTS 可能要 1~3 秒，
    # 否则板子会处在“已通知开始播放、却没有数据”的空窗状态。
    t0 = time.time()
    pcm = synth_speech(item.get("text", ""))
    print(f"[play] 合成完成 {len(pcm)/2/sr:.2f}s 音频，耗时 {time.time()-t0:.2f}s", flush=True)
    dev_set_state(device_id, "SPEAKING")
    _ws_send_json(ws, {"session_id": sid, "type": "audio_play", "state": "start",
                       "audio_params": {"format": "opus", "sample_rate": sr,
                                        "channels": 1, "frame_duration": fd}})
    print(f"[play] 开始下行播放 {item['id']}：{item.get('text', '')[:30]!r}", flush=True)
    if _OPUS_ENCODER is None:
        print("[play] 无 OPUS 编码器，跳过音频帧", flush=True)
    else:
        enc = _OPUS_ENCODER(sr, 1, "voip")
        off, total = 0, len(pcm)
        while off + samples * 2 <= total:
            if abort.is_set():
                break
            chunk = pcm[off: off + samples * 2]
            off += samples * 2
            try:
                opus = enc.encode(bytes(chunk), samples)
            except Exception as e:  # noqa: BLE001
                print(f"[play] 编码失败：{e}", flush=True)
                break
            pkt = audio_make_packet(opus)
            if not _ws_call(lambda p=pkt: ws.send(p)):
                break
            if abort.wait(fd / 1000.0):     # 严格 60ms 节奏，不突发
                break
    _ws_send_json(ws, {"session_id": sid, "type": "audio_play", "state": "stop"})
    with DEV_LOCK:
        PLAY_CURRENT.pop(device_id, None)
    cur = dev_get_state(device_id) or {}
    if cur.get("state") == "SPEAKING":
        dev_set_state(device_id, "IDLE")
    print(f"[play] 播放结束 {item['id']}", flush=True)
    _play_next(device_id)


def play_abort(device_id):
    """强制打断：立即停当前播放 + 清空队列（急停也走这里）。"""
    with DEV_LOCK:
        cur = PLAY_CURRENT.get(device_id)
        PLAY_QUEUE[device_id] = []
        if cur:
            cur["abort"].set()
    if cur:
        ws = dev_get_ws(device_id)
        if ws is not None:
            sid = (DEV_STATE.get(device_id) or {}).get("session_id") or ""
            _ws_send_json(ws, {"session_id": sid, "type": "audio_play", "state": "stop"})
        cur_state = dev_get_state(device_id) or {}
        if cur_state.get("state") == "SPEAKING":
            dev_set_state(device_id, "IDLE")
    return {"ok": True, "aborted": bool(cur)}


# ---------- 表情 ----------
def face_show(device_id, name, duration_ms=None):
    if name not in FACES:
        return {"ok": False, "error": "unknown face", "allowed": list(FACES)}
    dur = int(duration_ms or FACE_DEFAULT_MS)
    ok, reason, hint = dev_gate(device_id, "display.face")
    if not ok:
        return {"ok": False, "error": reason, "hint": hint}
    res = ws_command(device_id, "display.face", {"name": name, "duration_ms": dur},
                     mode=COMMAND_MODE.get("display.face", "async"),      # 协议示例为 async
                     policy=COMMAND_POLICY.get("display.face", "queue"))
    res["face"] = name
    return res


# ---------- OTA 两阶段握手 ----------
def _lan_ip():
    import socket as _sock
    try:
        s = _sock.socket(_sock.AF_INET, _sock.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:  # noqa: BLE001
        return "127.0.0.1"


def _latest_version():
    latest = FIRMWARE.get("__latest__")
    return (latest or {}).get("version") if isinstance(latest, dict) else latest


def ota_start(device_id, version=None, url=None):
    """触发 OTA。板端目前是预留实现（回 NOT_IMPLEMENTED 属正常），服务端同步预留。
    OTA 期间服务端门禁会拒绝一切其他命令。"""
    ver = version or _latest_version()
    fw = FIRMWARE.get(ver) if ver else None
    args = {"version": ver}
    if url:
        args["url"] = url
    elif fw:
        args.update({"url": f"http://{_lan_ip()}:{CTRL_PORT}/firmware/esp32s3/{ver}/app.bin",
                     "size": fw.get("size"), "sha256": fw.get("sha256")})
    dev_set_state(device_id, "UPGRADING")
    with DEV_LOCK:
        OTA_SESSION[device_id] = {"phase": "preparing", "version": ver, "since": time.time()}
    res = ws_command(device_id, "ota.start", args, mode="sync", policy="interrupt")
    with DEV_LOCK:
        s = OTA_SESSION.get(device_id) or {}
        s["phase"] = "downloading" if res.get("ok") else "failed"
        OTA_SESSION[device_id] = s
    res["phase"] = s["phase"]
    if not res.get("ok"):
        dev_set_state(device_id, "ERROR")
    return res


def ota_status(device_id):
    """查询板端 OTA 状态（预留）。"""
    return ws_command(device_id, "ota.status", {}, mode="sync", policy="queue")


def ota_ready(device_id, version=None):
    """阶段二：板子报“就绪”，服务端在响应体里直接返回下载地址。"""
    ver = (version or (OTA_SESSION.get(device_id) or {}).get("version")
           or _latest_version())
    fw = FIRMWARE.get(ver)
    if not fw:
        return {"ok": False, "error": "firmware not found", "version": ver}
    with DEV_LOCK:
        s = OTA_SESSION.get(device_id) or {}
        s.update({"phase": "downloading", "version": ver, "since": time.time()})
        OTA_SESSION[device_id] = s
    dev_set_state(device_id, "UPGRADING")
    return {"ok": True, "phase": "downloading", "version": ver,
            "url": f"http://{_lan_ip()}:{CTRL_PORT}/firmware/esp32s3/{ver}/app.bin",
            "size": fw.get("size"), "sha256": fw.get("sha256")}


def ota_finish(device_id, ok=True, version=None, note=""):
    with DEV_LOCK:
        s = OTA_SESSION.get(device_id) or {}
        s.update({"phase": "succeeded" if ok else "failed", "note": note,
                  "since": time.time()})
        OTA_SESSION[device_id] = s
    dev_set_state(device_id, "IDLE" if ok else "ERROR")
    return {"ok": True, "phase": s["phase"], "version": s.get("version")}

# [C-2.5] 设备端点注册表（新版双向架构：服务端主动 POST 到 ESP32 自己的 HTTP 服务 /api/v1/command）
# device_id -> {"host": str, "port": int, "token": str, "auto": bool, "last_seen": iso}
# 来源：① 控制台手动登记（POST /api/v1/device-endpoints）；② ESP32 连音频 WS 时自动用源 IP 注册(auto=True)
DEVICE_ENDPOINTS = {}
DEVICE_ENDPOINTS_FILE = os.path.join(_SCRIPT_DIR, "device_endpoints.json")


def load_device_endpoints():
    global DEVICE_ENDPOINTS
    try:
        if os.path.isfile(DEVICE_ENDPOINTS_FILE):
            with open(DEVICE_ENDPOINTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                DEVICE_ENDPOINTS = {k: v for k, v in data.items() if isinstance(v, dict)}
                print(f"[ctrl] 已加载 {len(DEVICE_ENDPOINTS)} 条设备端点（{DEVICE_ENDPOINTS_FILE}）")
    except Exception as e:  # noqa: BLE001
        print(f"[ctrl] 设备端点加载失败：{e}")


def save_device_endpoints():
    try:
        with open(DEVICE_ENDPOINTS_FILE, "w", encoding="utf-8") as f:
            json.dump(DEVICE_ENDPOINTS, f, ensure_ascii=False, indent=2)
    except Exception as e:  # noqa: BLE001
        print(f"[ctrl] 设备端点保存失败：{e}")


def device_endpoint_register(device_id, host, port=80, token="", auto=True, overwrite=False):
    """登记/更新设备端点。auto=True 为 WS 自动注册，默认不覆盖用户手动登记(overwrite=False)。"""
    if not device_id or not host:
        return False
    cur = DEVICE_ENDPOINTS.get(device_id)
    if cur and cur.get("auto") is False and not overwrite:
        return False  # 手动登记优先，自动注册不覆盖
    DEVICE_ENDPOINTS[device_id] = {
        "host": host, "port": int(port or 80), "token": token or "",
        "auto": bool(auto), "last_seen": _ctrl_now_iso(),
    }
    save_device_endpoints()
    return True


def device_send_command(device_id, command, args=None):
    """服务端作为 HTTP 客户端，主动 POST 到 ESP32 的 /api/v1/command（新双向架构）。
    返回 (ok, payload, status_code)。payload 为 ESP32 的 JSON 回复或错误信息。"""
    import urllib.request
    ep = DEVICE_ENDPOINTS.get(device_id)
    if not ep:
        return False, {"error": "no endpoint for device_id（请先在控制台登记设备端点，或让设备先连音频 WS 由源 IP 自动注册）"}, 0
    url = f"http://{ep['host']}:{ep['port']}/api/v1/command"
    envelope = {
        "version": 1,
        "request_id": _ctrl_gen_id(),
        "command": command,
        "args": args or {},
    }
    data = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if ep.get("token"):
        headers["Authorization"] = "Bearer " + ep["token"]
    try:
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", "replace")
            try:
                payload = json.loads(body)
            except Exception:
                payload = {"raw": body}
            return True, payload, resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace") if hasattr(e, "read") else str(e)
        try:
            payload = json.loads(body)
        except Exception:
            payload = {"raw": body}
        return False, payload, e.code
    except Exception as e:  # noqa: BLE001
        return False, {"error": f"{type(e).__name__}: {e}"}, 0
_CTRL_SEQ = 0


def _ctrl_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ctrl_gen_id():
    global _CTRL_SEQ
    _CTRL_SEQ += 1
    return ("cmd-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")
            + "-" + f"{_CTRL_SEQ:06d}")


def _ctrl_lan_ip():
    ips = local_ips()
    return ips[0] if ips else "127.0.0.1"


# ---------------------------------------------------------------------------
# [C-2] 加载器：固件索引 / 设备 token（main() 启动时调用一次）
# ---------------------------------------------------------------------------
def load_firmware_index():
    """扫描 firmware/esp32s3/<version>/app.bin，计算 size 与 sha256 建立固件表。
    可选 firmware/firmware.json: {"latest":"1.0.1"} 指明最新版本（缺省取最大版本号）。"""
    import hashlib
    FIRMWARE.clear()
    os.makedirs(os.path.join(FIRMWARE_DIR, "esp32s3"), exist_ok=True)
    fw_root = os.path.join(FIRMWARE_DIR, "esp32s3")
    for name in sorted(os.listdir(fw_root)):
        vdir = os.path.join(fw_root, name)
        binp = os.path.join(vdir, "app.bin")
        if os.path.isfile(binp):
            size = os.path.getsize(binp)
            h = hashlib.sha256()
            with open(binp, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
            FIRMWARE[name] = {"device": "esp32s3n16r8", "version": name,
                              "path": binp, "size": size, "sha256": h.hexdigest()}
    latest = None
    fj = os.path.join(FIRMWARE_DIR, "firmware.json")
    if os.path.isfile(fj):
        try:
            with open(fj, "r", encoding="utf-8") as f:
                meta = json.load(f)
            latest = meta.get("latest")
        except Exception:  # noqa: BLE001
            pass
    if not latest or latest not in FIRMWARE:
        vers = sorted(FIRMWARE.keys())
        latest = vers[-1] if vers else None
    FIRMWARE["__latest__"] = latest
    print(f"[ctrl] 固件表已载入：{len([k for k in FIRMWARE if k != '__latest__'])} 个版本，latest={latest}")


def load_device_tokens():
    """载入设备 token 表（{device_id: token}）。不指定文件则开发模式（接受任意 token）。"""
    global DEVICE_TOKENS
    DEVICE_TOKENS = {}
    if not DEVICE_TOKENS_FILE:
        return
    try:
        with open(DEVICE_TOKENS_FILE, "r", encoding="utf-8") as f:
            DEVICE_TOKENS = json.load(f)
        print(f"[ctrl] 已载入 {len(DEVICE_TOKENS)} 个设备 token（来自 {DEVICE_TOKENS_FILE}）")
    except Exception as e:  # noqa: BLE001
        print(f"[ctrl] 设备 token 文件读取失败：{e}，转为开发模式（接受任意 token）")


# ---------------------------------------------------------------------------
# [C-3] 指令队列：入队（管理端下发）/ 超时扫描（后台线程）
# 规则：一设备同时仅一个未完成指令；command_id 去重；60s 无状态上报标记超时。
# ---------------------------------------------------------------------------
def _ctrl_enqueue(device_id, type_, params):
    """管理端下发：创建指令并放入设备队列。返回指令 dict；若设备忙（已有未完成指令）返回 None。"""
    with CTRL_LOCK:
        if CMD_QUEUES.get(device_id) is not None:
            return None
        now = datetime.datetime.now(datetime.timezone.utc)
        created = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        expires = (now + datetime.timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
        cmd = {
            "id": _ctrl_gen_id(), "type": type_, "params": params or {},
            "created_at": created, "expires_at": expires,
            "status": "pending", "progress": 0, "message": "", "error_code": None,
            "device_id": device_id, "_updated": time.time(),
        }
        # OTA：若版本在固件表内，自动补全 url / size / sha256
        if type_ == "ota" and isinstance(params, dict):
            ver = params.get("version")
            f = FIRMWARE.get(ver) if ver else None
            if f:
                ip = _ctrl_lan_ip()
                cmd["params"] = {
                    "version": ver,
                    "url": f"http://{ip}:{CTRL_PORT}/firmware/esp32s3/{ver}/app.bin",
                    "size": f["size"], "sha256": f["sha256"],
                }
        CMD_QUEUES[device_id] = cmd
        return cmd


def _ctrl_timeout_scanner():
    """后台线程：每 10s 扫描，60s 内无状态更新的未完成指令标记为超时失败。"""
    while True:
        time.sleep(10)
        try:
            with CTRL_LOCK:
                for did, cmd in list(CMD_QUEUES.items()):
                    if cmd is None:
                        continue
                    if time.time() - cmd.get("_updated", 0) > 60:
                        print(f"[ctrl] 指令 {cmd['id']} 设备 {did} 超时（60s 无状态上报）")
                        cmd["status"] = "failed"
                        cmd["error_code"] = "CMD_TIMEOUT"
                        cmd["message"] = "timeout (no status in 60s)"
                        DEVICES.setdefault(did, {})["last_status"] = dict(cmd)
                        CMD_QUEUES[did] = None
        except Exception:  # noqa: BLE001
            pass


def _ctrl_cmd_public(cmd):
    """去掉内部字段后返回给设备的指令副本。"""
    if cmd is None:
        return None
    c = dict(cmd)
    c.pop("_updated", None)
    return c


# ---------------------------------------------------------------------------
# [C-4] HTTP 路由：ControlHandler（GET/POST 分发 + Bearer 鉴权 + 固件下载）
# 协议接口见模块 docstring 第 2 项；扩展接口 / 、/ctrl 、/voice 、/api/v1/devices。
# ---------------------------------------------------------------------------
class ControlHandler(http.server.BaseHTTPRequestHandler):
    server_version = "DogCtrl/1.0"

    def log_message(self, fmt, *args):
        try:
            print(f"[ctrl] {self.address_string()} - {fmt % args}")
        except Exception:  # noqa: BLE001
            pass

    # ---- 响应辅助 ----
    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text, code=200, ctype="text/plain; charset=utf-8"):
        body = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        try:
            n = int(self.headers.get("Content-Length", 0) or 0)
            if n <= 0:
                return {}
            raw = self.rfile.read(n)
            return json.loads(raw.decode("utf-8"))
        except Exception:  # noqa: BLE001
            return {}

    def _auth_ok(self, device_id=None):
        """鉴权：解析 Authorization: Bearer <token>。
        开发模式（未配置 token 表）接受任意非空 token；生产模式按 device_id 严格比对。"""
        auth = self.headers.get("Authorization", "")
        token = ""
        if auth.lower().startswith("bearer "):
            token = auth[7:].strip()
        if not token:
            return False
        if not DEVICE_TOKENS:
            return True
        if device_id is None:
            return token in DEVICE_TOKENS.values()  # 管理类端点：任意已知 token 即可
        return DEVICE_TOKENS.get(device_id) == token

    def _send_file(self, path, size):
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(size))
        self.end_headers()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def _serve_html(self, path=None):
        path = path or CTRL_HTML
        name = os.path.basename(path)
        if not os.path.isfile(path):
            return self._send_text(f"{name} 不存在（请将页面放到脚本同目录）", 404)
        with open(path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---- 入口 ----
    def do_GET(self):
        try:
            self._route("GET")
        except Exception as e:  # noqa: BLE001
            self._send_json({"ok": False, "error": str(e)}, 500)

    def do_POST(self):
        try:
            self._route("POST")
        except Exception as e:  # noqa: BLE001
            self._send_json({"ok": False, "error": str(e)}, 500)

    def do_DELETE(self):
        try:
            self._route("DELETE")
        except Exception as e:  # noqa: BLE001
            self._send_json({"ok": False, "error": str(e)}, 500)

    def _route(self, method):
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        parts = [p for p in path.split("/") if p != ""]
        # ===================== DELETE =====================
        if method == "DELETE":
            # /api/v1/device-endpoints/{id}
            if (len(parts) == 4 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "device-endpoints"):
                did = parts[3]
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                DEVICE_ENDPOINTS.pop(did, None)
                save_device_endpoints()
                return self._send_json({"ok": True, "endpoints": DEVICE_ENDPOINTS})
            return self._send_json({"ok": False, "error": "not found", "path": path}, 404)
        # ===================== GET =====================
        if method == "GET":
            if path == "/health":
                return self._send_json({"ok": True})
            # 前端自动识别实际端口（HTTP 控制端口 / WS 语音端口 / 模型状态），避免写死
            if path == "/api/v1/info":
                return self._send_json({
                    "ok": True,
                    "http_port": CTRL_PORT,
                    "ws_port": PORT,
                    "model": ASR_INFO,
                    "presets": {"http": [8000, 8001], "ws": [8765, 8766]},
                    "device_endpoints": DEVICE_ENDPOINTS,
                    "tts": TTS_NAME,
                    "brain": BRAIN_INFO,
                })
            # PC 语音输入运行时开关（仅限本机访问，无需 token）
            #   GET /pc-input/status  查看当前状态
            #   GET /pc-input/on      开启：识别结果 -> 剪贴板 -> Ctrl+V 粘贴到焦点窗口
            #   GET /pc-input/off     关闭：恢复为纯监测，不往电脑输入文字
            if path == "/pc-input" or path.startswith("/pc-input/"):
                global PC_INPUT
                host = self.client_address[0] if isinstance(self.client_address, tuple) else ""
                if host not in ("127.0.0.1", "::1", "::ffff:127.0.0.1"):
                    return self._send_json({"ok": False, "error": "localhost only"}, 403)
                sub = path[len("/pc-input"):].strip("/")
                if sub in ("", "status"):
                    return self._send_json({"ok": True, "enabled": PC_INPUT,
                                            "confirm": PC_INPUT_CONFIRM,
                                            "usage": "GET /pc-input/on 开启 · /pc-input/off 关闭"})
                if sub == "on":
                    PC_INPUT = True
                    print("[pc-input] 已开启：识别结果将粘贴到当前焦点窗口")
                    return self._send_json({"ok": True, "enabled": True,
                                            "confirm": PC_INPUT_CONFIRM})
                if sub == "off":
                    PC_INPUT = False
                    pending_pc_input.clear()
                    print("[pc-input] 已关闭：恢复为纯监测模式")
                    return self._send_json({"ok": True, "enabled": False})
                return self._send_json({"ok": False, "error": f"unknown action: {sub}"}, 404)
            # 管理：设备列表（含当前指令）
            # 大脑层状态：模型平台 / 是否配好 Key / 当前动作清单
            if path == "/api/v1/brain/status":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                info = dict(BRAIN_INFO)
                try:                       # 实时反映 llm_config.json 的最新内容
                    import llm_cloud
                    info.update(llm_cloud.describe())
                    info["config_file"] = llm_cloud.config_path()
                except Exception:  # noqa: BLE001
                    pass
                if BRAIN is not None:
                    info["persona"] = BRAIN.caps.get("persona", "")
                    info["max_reply_chars"] = BRAIN.caps.get("max_reply_chars", 30)
                return self._send_json({"ok": True, "brain": info})
            # 调试台：读写大模型配置（provider / model / key），写完热生效
            if path == "/api/v1/brain/config":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                try:
                    return self._send_json({"ok": True, "config": _brain_config_dict()})
                except Exception as e:  # noqa: BLE001
                    return self._send_json({"ok": False, "error": str(e)}, 500)
            # 调试台：能力清单原文（persona / rules / actions / faces）
            if path == "/api/v1/brain/caps":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                try:
                    with open(os.path.join(_SCRIPT_DIR, "dog_actions.json"),
                              "r", encoding="utf-8") as f:
                        caps = json.load(f)
                    return self._send_json({"ok": True, "caps": caps,
                                            "file": os.path.join(_SCRIPT_DIR,
                                                                 "dog_actions.json")})
                except Exception as e:  # noqa: BLE001
                    return self._send_json({"ok": False, "error": str(e)}, 500)
            # 调试台：某个设备的对话历史（模型串味时看这里）
            if path == "/api/v1/brain/history":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                q = urllib.parse.parse_qs(parsed.query or "")
                did = (q.get("device_id") or [""])[0].strip()
                if BRAIN is None:
                    return self._send_json({"ok": False, "error": "brain 未启用"}, 503)
                return self._send_json({"ok": True, "device_id": did,
                                        "history": BRAIN.history(did)})
            if path == "/api/v1/devices":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                with CTRL_LOCK:
                    devs_map = {did: dict(d) for did, d in DEVICES.items()}
                # 新架构下板子走 WebSocket（?role=dog）而不再发 HTTP 心跳，
                # 仅看 DEVICES 会让这张表永远为空；这里把 WS 侧登记过的设备一并合并。
                for did in dev_state_all():
                    st = dev_get_state(did) or {}
                    cur = DEV_CURRENT.get(did) or {}
                    online = bool(st.get("ws_alive"))
                    row = devs_map.setdefault(did, {})
                    row["ip"] = st.get("ip") or row.get("ip")
                    row.setdefault("rssi", None)
                    row.setdefault("free_heap", None)
                    row["online"] = online
                    row["state"] = st.get("state", "OFFLINE")
                    row["age"] = st.get("age")
                    row["source"] = "ws" if (online or st.get("session_id")) else row.get("source", "endpoint")
                    row["last_heartbeat"] = st.get("last_seen", row.get("last_heartbeat"))
                    row["busy"] = bool(row.get("busy") or cur)
                    row["running_slot"] = cur.get("command", row.get("running_slot"))
                devs = []
                for did, d in sorted(devs_map.items()):
                    c = CMD_QUEUES.get(did)
                    devs.append({"device_id": did, **d, "command": _ctrl_cmd_public(c)})
                return self._send_json({"ok": True, "devices": devs})
            # 固件 latest
            if path == "/api/v1/firmware/esp32s3/latest":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                latest = FIRMWARE.get("__latest__")
                if not latest or latest not in FIRMWARE:
                    return self._send_json({"ok": False, "error": "no firmware"}, 404)
                f = FIRMWARE[latest]
                return self._send_json({"ok": True, "device": f["device"], "version": f["version"],
                                        "url": f"http://{_ctrl_lan_ip()}:{CTRL_PORT}/firmware/esp32s3/{latest}/app.bin",
                                        "size": f["size"], "sha256": f["sha256"]})
            # 固件版本列表
            if path == "/api/v1/firmware/esp32s3":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                vers = [FIRMWARE[k] for k in FIRMWARE if k != "__latest__"]
                return self._send_json({"ok": True, "latest": FIRMWARE.get("__latest__"),
                                        "versions": vers})
            # 设备端点注册表（新版双向架构：服务端直推 ESP32）
            if path == "/api/v1/devices/state":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                return self._send_json({
                    "ok": True, "devices": dev_state_all(),
                    "states": list(DEV_STATES), "faces": list(FACES),
                    "queue": {d: len(q) for d, q in PLAY_QUEUE.items() if q},
                    "current": DEV_CURRENT, "ota": OTA_SESSION})

            if path == "/api/v1/device-endpoints":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                return self._send_json({"ok": True, "endpoints": DEVICE_ENDPOINTS})
            # /api/v1/devices/{did}/commands/next
            if (len(parts) == 6 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "devices" and parts[4] == "commands" and parts[5] == "next"):
                did = parts[3]
                if not self._auth_ok(did):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                with CTRL_LOCK:
                    cmd = CMD_QUEUES.get(did)
                    if cmd is not None and cmd.get("status") == "pending":
                        c = _ctrl_cmd_public(cmd)
                        CMD_QUEUES[did] = dict(cmd, status="accepted", _updated=time.time())
                        return self._send_json({"ok": True, "command": c}, 200)
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            # /firmware/esp32s3/{version}/app.bin
            if (len(parts) == 4 and parts[0] == "firmware" and parts[1] == "esp32s3"
                    and parts[3] == "app.bin"):
                ver = parts[2]
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                f = FIRMWARE.get(ver)
                if not f or not os.path.isfile(f["path"]):
                    return self._send_json({"ok": False, "error": "firmware not found"}, 404)
                return self._send_file(f["path"], f["size"])
            # ===== 数字孪生主控台（从板端页面搬到服务端）=====
            if path.startswith("/api/v1/twin/"):
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                if not twin_available():
                    return self._send_json(
                        {"ok": False, "error": "twin.py 未加载，孪生功能不可用"}, 503)
                q = urllib.parse.parse_qs(parsed.query)
                did = twin_pick_device(q.get("device_id", [""])[0])

                if path == "/api/v1/twin/store":
                    return self._send_json({"ok": True, "store": twin_mod.load(),
                                            "store_path": twin_mod.STORE_PATH})
                if path == "/api/v1/twin/status":
                    return self._send_json(twin_status_payload())
                if path == "/api/v1/twin/gait":
                    return self._send_json({"ok": True, "gait": TWIN_GAIT.state(),
                                            "sequence": TWIN_SEQ.state()})
                if path == "/api/v1/twin/servo/pos":
                    ids = [int(x) for x in re.findall(r"\d+", q.get("ids", [""])[0])
                           if 0 < int(x) < 254][:32]
                    if not ids:
                        return self._send_json({"ok": False, "error": "ids 必填"}, 400)
                    if not did:
                        return self._send_json({"ok": False, "error": "没有可用设备"}, 409)
                    r = twin_positions(did, ids)
                    r["device_id"] = did
                    return self._send_json(r, 200 if r.get("ok") else 409)
                if path == "/api/v1/twin/battery":
                    if not did:
                        return self._send_json({"ok": False, "error": "没有可用设备"}, 409)
                    r = twin_battery(did)
                    r["device_id"] = did
                    return self._send_json(r, 200 if r.get("ok") else 409)
                return self._send_json({"ok": False, "error": "not found", "path": path}, 404)
            # 聚合首页 / 管理页 / 语音监测台
            if path in ("/", "/portal", "/portal.html"):
                return self._serve_html(PORTAL_HTML)
            if path in ("/ctrl", "/ctrl.html", "/index_ctrl.html"):
                return self._serve_html(CTRL_HTML)
            if path in ("/brain", "/brain.html"):
                return self._serve_html(BRAIN_HTML)
            if path in ("/twin", "/twin.html"):
                return self._serve_html(TWIN_HTML)
            if path in ("/voice", "/voice.html", "/index.html"):
                return self._serve_html(INDEX_HTML)
            return self._send_json({"ok": False, "error": "not found", "path": path}, 404)
        # ===================== POST =====================
        if method == "POST":
            # /api/v1/brain/say —— 对话入口：不用板端说话也能验证整条链路
            if (len(parts) == 4 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "brain" and parts[3] == "say"):
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                body = self._read_json() or {}
                did = (body.get("device_id") or os.environ.get("DOG_BRAIN_DEVICE") or "").strip()
                text = (body.get("text") or "").strip()
                if not did:
                    return self._send_json({"ok": False,
                                            "error": "device_id 必填（或设 DOG_BRAIN_DEVICE）"}, 400)
                if not text:
                    return self._send_json({"ok": False, "error": "text 必填"}, 400)
                res = brain_say(did, text, wait=bool(body.get("wait")))
                res.update({"device_id": did, "text": text})
                return self._send_json(res, 200 if res.get("ok") else 503)
            # 调试台：预演一句话（走完整决策但不下发动作/播报）
            if (len(parts) == 4 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "brain" and parts[3] == "test"):
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                body = self._read_json() or {}
                did = (body.get("device_id") or os.environ.get("DOG_BRAIN_DEVICE")
                       or "debug").strip()
                text = (body.get("text") or "").strip()
                if not text:
                    return self._send_json({"ok": False, "error": "text 必填"}, 400)
                res = _brain_dry_run(did, text)
                res.update({"device_id": did, "text": text})
                return self._send_json(res, 200 if res.get("ok") else 503)
            # 调试台：保存大模型配置（写 llm_config.json，免重启）
            if (len(parts) == 4 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "brain" and parts[3] == "config"):
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                body = self._read_json() or {}
                try:
                    cfg = _brain_write_config(body)
                except Exception as e:  # noqa: BLE001
                    return self._send_json({"ok": False, "error": str(e)}, 500)
                if BRAIN is not None:            # 能力清单没变，但配置变了要记一笔
                    print(f"[brain] 配置已更新：{cfg.get('provider')} / {cfg.get('model')}"
                          f" ready={cfg.get('ready')}", flush=True)
                return self._send_json({"ok": True, "config": cfg})
            # 调试台：连通自检（Key / 端点 / 模型名对不对）
            if (len(parts) == 4 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "brain" and parts[3] == "ping"):
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                r = _brain_ping()
                return self._send_json(r, 200 if r.get("ok") else 503)
            # 调试台：重读 dog_actions.json（改完动作清单不用重启服务）
            if (len(parts) == 4 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "brain" and parts[3] == "reload"):
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                if BRAIN is None:
                    return self._send_json({"ok": False, "error": "brain 未启用"}, 503)
                BRAIN.reload()
                return self._send_json({"ok": True, "actions": BRAIN.action_names(),
                                        "faces": BRAIN.caps.get("faces", [])})
            # 调试台：清空某设备的对话历史
            if (len(parts) == 5 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "brain" and parts[3] == "history"
                    and parts[4] == "clear"):
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                body = self._read_json() or {}
                did = str(body.get("device_id") or "").strip()
                if BRAIN is None:
                    return self._send_json({"ok": False, "error": "brain 未启用"}, 503)
                n = BRAIN.clear_history(did)
                return self._send_json({"ok": True, "cleared": n, "device_id": did})
            # ===== 数字孪生主控台：写接口（全部转发到 WS 命令层）=====
            if path.startswith("/api/v1/twin/"):
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                if not twin_available():
                    return self._send_json(
                        {"ok": False, "error": "twin.py 未加载，孪生功能不可用"}, 503)
                body = self._read_json() or {}
                did = twin_pick_device(body.get("device_id"))

                # 保存标定 / 几何 / 步态参数 / 动作组：只换传进来的键，其余保持原样
                if path == "/api/v1/twin/store":
                    patch = body.get("patch") if isinstance(body.get("patch"), dict) else body
                    try:
                        store = twin_mod.save(patch)
                    except Exception as e:  # noqa: BLE001
                        return self._send_json({"ok": False, "error": str(e)}, 400)
                    return self._send_json({"ok": True, "store": store})

                if path == "/api/v1/twin/store/reset":
                    try:
                        store = twin_mod.reset_section((body.get("section") or "").strip())
                    except Exception as e:  # noqa: BLE001
                        return self._send_json({"ok": False, "error": str(e)}, 400)
                    return self._send_json({"ok": True, "store": store})

                if path in ("/api/v1/twin/servo/move", "/api/v1/twin/servo/unload",
                            "/api/v1/twin/sequence", "/api/v1/twin/sequence/stop",
                            "/api/v1/twin/gait/start", "/api/v1/twin/gait/stop",
                            "/api/v1/twin/estop") and not did:
                    return self._send_json(
                        {"ok": False, "error": "没有可用设备：板子需先连上音频 WS"},
                        409)

                if path == "/api/v1/twin/servo/move":
                    servos = body.get("servos")
                    if not isinstance(servos, list) or not servos:
                        return self._send_json({"ok": False, "error": "servos 必填"}, 400)
                    r = servo_dispatch(did, "servo.move", {"servos": servos},
                                       policy="interrupt")
                    r["device_id"] = did
                    return self._send_json(r, 200 if r.get("ok") else 409)

                if path == "/api/v1/twin/servo/unload":
                    ids = body.get("ids")
                    ids = [int(x) for x in ids] if isinstance(ids, list) else []
                    if not ids:
                        return self._send_json({"ok": False, "error": "ids 必填"}, 400)
                    r = servo_dispatch(did, "servo.unload", {"ids": ids},
                                       policy="interrupt")
                    r["device_id"] = did
                    return self._send_json(r, 200 if r.get("ok") else 409)

                if path == "/api/v1/twin/sequence":
                    steps = body.get("steps")
                    if not isinstance(steps, list) or not steps:
                        return self._send_json({"ok": False, "error": "steps 必填"}, 400)
                    repeat = body.get("repeat", 1)
                    if repeat is True:
                        repeat = 0                      # true = 不限次数
                    try:
                        repeat = int(repeat)
                    except (TypeError, ValueError):
                        repeat = 1
                    if repeat < 0:
                        repeat = 0
                    if repeat == 1:
                        r = servo_dispatch(did, "sequence.run", {"steps": steps})
                        r["device_id"] = did
                        return self._send_json(r, 200 if r.get("ok") else 409)
                    # 重复播放交给服务端后台线程：页面关掉也能一直跑
                    ok, reason, hint = twin_can_take_over(did, "twin.sequence")
                    if not ok:
                        return self._send_json({"ok": False, "error": reason,
                                                "hint": hint}, 409)
                    try:
                        gap = int(body.get("gap_ms") or 50)
                    except (TypeError, ValueError):
                        gap = 50
                    r = TWIN_SEQ.start(did, steps, total_rounds=repeat, gap_ms=gap)
                    r["device_id"] = did
                    r["est_round_ms"] = twin_mod.sequence_duration_ms(steps)
                    return self._send_json(r, 200 if r.get("ok") else 409)

                if path == "/api/v1/twin/sequence/stop":
                    TWIN_SEQ.stop()
                    r = servo_dispatch(did, "sequence.stop", {})
                    r["device_id"] = did
                    return self._send_json(r, 200 if r.get("ok") else 409)

                if path == "/api/v1/twin/gait/start":
                    ok, reason, hint = twin_can_take_over(did, "twin.gait")
                    if not ok:
                        return self._send_json({"ok": False, "error": reason,
                                                "hint": hint}, 409)
                    params = body.get("params")
                    r = TWIN_GAIT.start(did, params if isinstance(params, dict) else None)
                    r["device_id"] = did
                    return self._send_json(r, 200 if r.get("ok") else 409)

                if path == "/api/v1/twin/gait/stop":
                    r = TWIN_GAIT.stop(reason="user")
                    r["device_id"] = did
                    return self._send_json(r, 200 if r.get("ok") else 409)

                if path == "/api/v1/twin/estop":
                    twin_stop_all(did, reason="estop")
                    r = servo_dispatch(did, "motion.emergency_stop", {})
                    r["device_id"] = did
                    return self._send_json(r, 200 if r.get("ok") else 409)

                return self._send_json({"ok": False, "error": "not found", "path": path}, 404)
            # /api/v1/devices/{did}/commands/{cid}/status
            if (len(parts) == 7 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "devices" and parts[4] == "commands"
                    and parts[6] == "status"):
                did, cid = parts[3], parts[5]
                body = self._read_json()
                if not self._auth_ok(did):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                with CTRL_LOCK:
                    cmd = CMD_QUEUES.get(did)
                    if cmd is None or cmd.get("id") != cid:
                        return self._send_json({"ok": False, "error": "command not found"}, 404)
                    if "status" in body:
                        cmd["status"] = body["status"]
                    if "progress" in body:
                        cmd["progress"] = body["progress"]
                    if "message" in body:
                        cmd["message"] = body["message"]
                    if "error_code" in body:
                        cmd["error_code"] = body["error_code"]
                    cmd["_updated"] = time.time()
                    terminal = cmd["status"] in ("succeeded", "failed", "rollback")
                    if terminal:
                        DEVICES.setdefault(did, {})["last_status"] = _ctrl_cmd_public(cmd)
                        CMD_QUEUES[did] = None
                return self._send_json({"ok": True})
            # /api/v1/devices/{did}/heartbeat
            if (len(parts) == 5 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "devices" and parts[4] == "heartbeat"):
                did = parts[3]
                body = self._read_json()
                if not self._auth_ok(did):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                with CTRL_LOCK:
                    d = DEVICES.setdefault(did, {})
                    for k in ("fw_version", "running_slot", "ip", "rssi",
                              "uptime_sec", "free_heap", "psram_free", "busy"):
                        if k in body:
                            d[k] = body[k]
                    d["last_heartbeat"] = _ctrl_now_iso()
                return self._send_json({"ok": True, "server_time": _ctrl_now_iso()})
            # /api/v1/devices/{did}/commands  (管理端下发)
            if (len(parts) == 5 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "devices" and parts[4] == "commands"):
                did = parts[3]
                body = self._read_json()
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                t = body.get("type")
                if t not in ("servo_action", "action_group", "stop", "reboot", "ota"):
                    return self._send_json({"ok": False, "error": "unknown command type"}, 400)
                params = body.get("params", {})
                cmd = _ctrl_enqueue(did, t, params)
                if cmd is None:
                    return self._send_json({"ok": False,
                                            "error": "device busy, one command at a time"}, 409)
                print(f"[ctrl] 已下发指令 {cmd['id']} 类型 {t} -> 设备 {did}")
                return self._send_json({"ok": True, "command": _ctrl_cmd_public(cmd)}, 200)
            # ===== 新版双向架构：设备端点手动登记（控制台填写 ESP32 的 IP:端口:token）=====
            # ---------- [C-6] 调度中枢接口（下行控制 / 状态 / 播放 / OTA）----------
            _P = "/api/v1/devices/"
            if path.startswith(_P):
                tail = path[len(_P):]
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                body = self._read_json() or {}

                if tail.endswith("/command"):
                    did = tail[:-len("/command")]
                    cmd = body.get("command")
                    if not cmd:
                        return self._send_json({"ok": False, "error": "missing command"}, 400)
                    res = servo_dispatch(did, cmd, body.get("args") or {},
                                         priority=body.get("priority"),
                                         policy=body.get("policy"),
                                         protect=bool(body.get("protect")),
                                         mode=body.get("mode"))   # None -> 取 COMMAND_MODE 默认
                    return self._send_json(res, 200 if res.get("ok") else 409)

                if tail.endswith("/state"):
                    did = tail[:-len("/state")]
                    stt = str(body.get("state", "IDLE")).upper()
                    changed = dev_set_state(did, stt, ip=self.client_address[0])
                    return self._send_json({"ok": True, "device_id": did,
                                            "state": stt, "changed": changed})

                if tail.endswith("/face"):
                    did = tail[:-len("/face")]
                    res = face_show(did, body.get("name", ""), body.get("duration_ms"))
                    return self._send_json(res, 200 if res.get("ok") else 409)

                if tail.endswith("/play"):
                    did = tail[:-len("/play")]
                    res = audio_play_text(did, body.get("text", ""))
                    return self._send_json(res, 200 if res.get("ok") else 409)

                if tail.endswith("/audio/abort"):
                    did = tail[:-len("/audio/abort")]
                    return self._send_json(play_abort(did))

                if tail.endswith("/ota/start"):
                    did = tail[:-len("/ota/start")]
                    res = ota_start(did, body.get("version"), body.get("url"))
                    return self._send_json(res, 200 if res.get("ok") else 409)

                if tail.endswith("/ota/status"):
                    did = tail[:-len("/ota/status")]
                    return self._send_json(ota_status(did))

                if tail.endswith("/ota/ready"):
                    did = tail[:-len("/ota/ready")]
                    return self._send_json(ota_ready(did, body.get("version")))

                if tail.endswith("/ota/finish"):
                    did = tail[:-len("/ota/finish")]
                    return self._send_json(ota_finish(
                        did, ok=bool(body.get("ok", True)),
                        version=body.get("version"), note=body.get("note", "")))

            if path == "/api/v1/device-endpoints":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                body = self._read_json()
                did = body.get("device_id")
                host = body.get("host")
                if not did or not host:
                    return self._send_json({"ok": False, "error": "device_id & host required"}, 400)
                if device_endpoint_register(did, host, body.get("port", 80),
                                            body.get("token", ""), auto=False, overwrite=True):
                    return self._send_json({"ok": True, "endpoint": DEVICE_ENDPOINTS.get(did),
                                            "endpoints": DEVICE_ENDPOINTS})
                return self._send_json({"ok": False, "error": "register failed"}, 500)
            # ===== 新版双向架构：服务端主动 POST 到 ESP32 的 /api/v1/command =====
            if (len(parts) == 5 and parts[0] == "api" and parts[1] == "v1"
                    and parts[2] == "devices" and parts[4] == "command"):
                did = parts[3]
                body = self._read_json()
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                command = body.get("command")
                if not command:
                    return self._send_json({"ok": False, "error": "missing command"}, 400)
                ok, reply, code = device_send_command(did, command, body.get("args", {}))
                return self._send_json({"ok": ok, "device_id": did,
                                        "esp32_status": code, "reply": reply},
                                       200 if ok else 502)
            return self._send_json({"ok": False, "error": "not found", "path": path}, 404)


# ---------------------------------------------------------------------------
# [C-5] 服务启动：ThreadingHTTPServer 守护线程（:8000）
# ---------------------------------------------------------------------------
def start_ctrl_server():
    """启动设备控制/OTA HTTP 服务（独立线程）。"""
    http.server.ThreadingHTTPServer((CTRL_HOST, CTRL_PORT), ControlHandler).serve_forever()


# ---------------------------------------------------------------------------
# 工具：本机局域网 IP 探测（仅供打印提示）
# ---------------------------------------------------------------------------
def local_ips():
    """拿到本机所有局域网 IP，方便用户核对 ESP32 里的 URL 写没写对。"""
    ips = []
    try:
        # 先拿到一个能连外网的默认 IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0)
        try:
            s.connect(("8.8.8.8", 1))
            ips.append(s.getsockname()[0])
        except Exception:  # noqa: BLE001
            pass
        finally:
            s.close()
        # 再枚举所有接口
        hn = socket.gethostname()
        for a in socket.getaddrinfo(hn, None, socket.AF_INET, socket.SOCK_STREAM):
            ip = a[4][0]
            if ip not in ips and not ip.startswith("127."):
                ips.append(ip)
    except Exception:  # noqa: BLE001
        pass
    return ips


# ===========================================================================
# [D] 入口：main() —— 统一编排两套服务（控制先于语音；--ctrl-only 时仅控制）
# ===========================================================================
async def main():
    global LOOP, PORT, HTTP_PORT, MODEL, ENGINE, DEVICE, COMPUTE, BUTTON_GAP_S, SAVE_WAV, DENOISE, VAD, PC_INPUT, PC_INPUT_CONFIRM
    global CTRL_ONLY, CTRL_ENABLED, CTRL_HOST, CTRL_PORT, FIRMWARE_DIR, CTRL_HTML, DEVICE_TOKENS_FILE
    LOOP = asyncio.get_running_loop()
    # ---- 设备控制 / OTA 服务（:8000）----
    load_firmware_index()
    load_device_tokens()
    load_device_endpoints()
    _load_tts()          # 接得上 edge-tts 就用真语音，接不上自动回退提示音
    _load_brain()        # 大脑层：DOG_BRAIN=1 且配好 LLM_API_KEY 才真正可用
    if CTRL_ENABLED:
        threading.Thread(target=start_ctrl_server, daemon=True).start()
        threading.Thread(target=_ctrl_timeout_scanner, daemon=True).start()
        print(f"[ctrl] 服务总入口（聚合首页）: http://{CTRL_HOST}:{CTRL_PORT}/  (管理台 /ctrl · 语音监测台 /voice)")
        if CTRL_ONLY:
            print("[ctrl] --ctrl-only：仅运行控制服务，按 Ctrl+C 停止。")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n[ctrl] 已停止，Bye.")
            return
    load_asr()
    if _OPUS_DECODER is None:
        print("[xz] 警告：未安装 opuslib_next，无法解码 xiaozhi 采集端的 OPUS 音频。")
        print("    安装：pip install opuslib_next  （Windows 还需 opus.dll 在 PATH，")
        print("          推荐 conda install -c conda-forge opus 或把 opus.dll 放到目录）")
    await broadcast_asr_status()
    threading.Thread(target=asr_worker, daemon=True).start()
    print(f"[server] 语音监测台页面（由控制服务统一托管）: http://localhost:{CTRL_PORT}/voice")
    print(f"[server] WebSocket :{PORT}  (?role=mic 麦克风 / ?role=dash 监测 / ?role=xiaozhi 小智采集端 / ?role=dog 机器狗采集端)")
    print(f"[server] PCM={SAMPLE_RATE}Hz int16 单声道 | 按钮间隙={BUTTON_GAP_S}s")
    ip_list = local_ips()
    print(f"[server] 本机可用 IP: {', '.join(ip_list) if ip_list else '未能自动获取'}   "
          "ESP32 URL 里请填实际 IP，不要用 localhost/127.0.0.1")
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()  # 永久运行


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="机器狗 PC 服务端：语音 ASR(WS:8765) + 设备控制/OTA(HTTP:8000)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--http-port", type=int, default=HTTP_PORT,
                        help="[遗留] 旧独立监测台端口(:8080)，已废弃；语音监测台现由控制服务在 :8000/voice 提供，本参数不再生效")
    parser.add_argument("--engine", choices=["funasr", "whisper"], default=ENGINE,
                        help="ASR 引擎：funasr（默认，SenseVoice 中文流式+抗噪强）/ whisper（做对比）")
    parser.add_argument("--model", default=MODEL,
                        help="FunASR 模型 id（如 iic/SenseVoiceSmall）或本地路径；whisper 引擎时填 base/tiny/small")
    parser.add_argument("--device", default=DEVICE, help="cpu / cuda")
    parser.add_argument("--compute", default=COMPUTE, help="int8 / float16 ...（仅 whisper 引擎使用）")
    parser.add_argument("--gap", type=float, default=BUTTON_GAP_S, help="按钮松开判定间隔(秒)")
    parser.add_argument("--save-wav", action="store_true",
                        help="把每段提交的音频存成 record_*.wav（同时存 _denoised 降噪版用于 A/B）")
    parser.add_argument("--no-denoise", action="store_true",
                        help="关闭 50Hz 陷波+高通 软件降噪（用于和原始输入做对比）")
    parser.add_argument("--no-vad", action="store_true",
                        help="关闭 Whisper VAD（强电流麦下 VAD 会把语音当噪声剪掉，导致识别为空；"
                             "此时用整段音频送识别，配合 --no-denoise 或默认降噪做 A/B）")
    parser.add_argument("--pc-input", action="store_true",
                        help="PC 语音输入模式：识别结果写剪贴板并模拟 Ctrl+V 粘贴到当前焦点窗口光标处（松手即发）")
    parser.add_argument("--pc-input-confirm", action="store_true",
                        help="PC 语音输入（确认模式）：识别后先缓存不发送，等板子发 "
                             "{\"type\":\"pc_input\",\"action\":\"confirm\"} 才粘贴到焦点窗口，避免误输入")
    parser.add_argument("--ctrl-only", action="store_true",
                        help="仅启动设备控制/OTA 服务(:8000)，不加载 ASR、不起 WebSocket（方便在没有 torch 的机器上调试控制服务）")
    parser.add_argument("--no-ctrl", action="store_true",
                        help="不启动设备控制/OTA 服务（仅 ASR）")
    parser.add_argument("--ctrl-host", default=CTRL_HOST, help="控制服务监听地址（默认 0.0.0.0）")
    parser.add_argument("--ctrl-port", type=int, default=CTRL_PORT, help="控制服务端口（默认 8000）")
    parser.add_argument("--firmware-dir", default=FIRMWARE_DIR, help="固件目录（默认 ./firmware）")
    parser.add_argument("--ctrl-html", default=CTRL_HTML, help="管理页文件（默认 ./ctrl.html）")
    parser.add_argument("--device-tokens", default=None,
                        help="设备 token 表 JSON（{device_id: token}）；不指定则开发模式接受任意 token")
    a = parser.parse_args()

    PORT = a.port
    HTTP_PORT = a.http_port
    MODEL = a.model
    ENGINE = a.engine
    DEVICE = a.device
    COMPUTE = a.compute
    BUTTON_GAP_S = a.gap
    SAVE_WAV = a.save_wav
    DENOISE = not a.no_denoise
    VAD = not a.no_vad
    PC_INPUT = a.pc_input or a.pc_input_confirm
    PC_INPUT_CONFIRM = a.pc_input_confirm
    CTRL_ONLY = a.ctrl_only
    CTRL_ENABLED = not a.no_ctrl
    CTRL_HOST = a.ctrl_host
    CTRL_PORT = a.ctrl_port
    FIRMWARE_DIR = a.firmware_dir
    CTRL_HTML = a.ctrl_html
    DEVICE_TOKENS_FILE = a.device_tokens
    if PC_INPUT and os.name != "nt":
        print("[pc-input] 警告：非 Windows 平台，--pc-input / --pc-input-confirm 无效")
        PC_INPUT = False
        PC_INPUT_CONFIRM = False

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[server] 已停止，Bye.")
