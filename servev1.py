#!/usr/bin/env python3
"""PC 语音服务器（仅 ASR） —— 机器狗麦克风端 ESP32 的上位机。

协议（按需求规格）
-----------------
  mic ESP32  --(WebSocket 二进制, 纯 PCM, ?role=mic)-->  本服务器  --FunASR SenseVoice / faster-whisper-->  中文原文
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
except Exception:  # noqa: BLE001
    opuslib_next = None
    _OPUS_DECODER = None

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

clients = {"mic": set(), "dash": set(), "xz": set()}   # xz = xiaozhi 采集端
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
# 三类客户端：?role=mic（裸 PCM）/ ?role=xiaozhi（OPUS）/ ?role=dash（监测）
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
    print(f"[{role}] 已连接（mic={len(clients['mic'])} xz={len(xz_state)} dash={len(clients['dash'])}）")

    if role == "dash":
        await broadcast_dash({
            "type": "status", "mic": "connected" if mic_connected() else "disconnected",
            "button": False, "recording": False, "level": 0.0, "buffer_ms": 0,
        })
        await broadcast_asr_status()
        await broadcast_mic_status()   # 主动补推一次连接状态（前端可能晚于事件连入）

    is_xz = (role == "xz")
    mon = None
    if role == "mic":
        mic_state[ws] = {"buf": bytearray(), "last": 0.0, "recording": False, "level": 0.0, "ending": False}
        mon = asyncio.create_task(monitor(ws))
        await broadcast_dash({"type": "event", "msg": "麦克风已连接"})
        await broadcast_mic_status()
    elif is_xz:
        xz_state[ws] = {"buf": bytearray(), "last": 0.0, "recording": False,
                        "level": 0.0, "ending": False, "mode": "xz", "version": 0}
        await broadcast_dash({"type": "event", "msg": "xiaozhi 采集端已连接，等待 hello"})
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
                        msg_dbg = f"[xz-dbg #{st_dbg['dbg_n']}] {prev}"
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
            with pending_lock:
                pending_pc_input.pop(ws, None)   # 清理该连接的待确认文本，避免内存泄漏
            clients["xz"].discard(ws)
            await broadcast_dash({"type": "event", "msg": "xiaozhi 采集端断开"})
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
    if t == "listen":
        state = j.get("state")
        if state == "start":
            st["buf"] = bytearray()
            st["recording"] = True
            st["ending"] = False
            st["last"] = time.monotonic()
            print("[xz] 开始说话（listen start）")
            await broadcast_dash({"type": "event", "msg": "开始说话（按键/唤醒）"})
            await broadcast_mic_status()
        elif state == "stop":
            print("[xz] 停止（listen stop）")
            await broadcast_dash({"type": "event", "msg": "提交识别（listen stop）"})
            submit_buffer(ws, st)
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

    def _route(self, method):
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        parts = [p for p in path.split("/") if p != ""]
        # ===================== GET =====================
        if method == "GET":
            if path == "/health":
                return self._send_json({"ok": True})
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
            if path == "/api/v1/devices":
                if not self._auth_ok(None):
                    return self._send_json({"ok": False, "error": "unauthorized"}, 401)
                with CTRL_LOCK:
                    devs = []
                    for did, d in DEVICES.items():
                        c = CMD_QUEUES.get(did)
                        devs.append({"device_id": did, **d,
                                     "command": _ctrl_cmd_public(c)})
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
            # 聚合首页 / 管理页 / 语音监测台
            if path in ("/", "/portal", "/portal.html"):
                return self._serve_html(PORTAL_HTML)
            if path in ("/ctrl", "/ctrl.html", "/index_ctrl.html"):
                return self._serve_html(CTRL_HTML)
            if path in ("/voice", "/voice.html", "/index.html"):
                return self._serve_html(INDEX_HTML)
            return self._send_json({"ok": False, "error": "not found", "path": path}, 404)
        # ===================== POST =====================
        if method == "POST":
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
    print(f"[server] WebSocket :{PORT}  (?role=mic 麦克风 / ?role=dash 监测 / ?role=xiaozhi 小智采集端)")
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
