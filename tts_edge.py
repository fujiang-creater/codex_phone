# -*- coding: utf-8 -*-
"""TTS 适配层：edge-tts（微软 Edge 在线神经网络语音）→ 服务端要的 PCM。

契约（与 servev1.py 的 TTS_HOOK 一致）：
    synth(text) -> bytes        # 16000Hz / 单声道 / int16 裸 PCM，不带 WAV 头

edge-tts 出来的是 24kHz 的 MP3，而下行播放是按 16kHz 分 60ms 帧编码的，
所以这里必须做解码 + 重采样，否则播出来会变调变速。
环境没有 ffmpeg 可执行文件，解码和重采样都用已装的 PyAV（自带 ffmpeg 库）。
"""
import asyncio
import hashlib
import io
import os
import threading

TARGET_SR = 16000

# 音色可用环境变量覆盖：set TTS_VOICE=zh-CN-YunxiNeural
# 注意 edge-tts 的参数校验：rate/volume 用百分比（+10%），pitch 必须用赫兹（+10Hz）
VOICE = os.environ.get("TTS_VOICE", "zh-CN-XiaoxiaoNeural")
RATE = os.environ.get("TTS_RATE", "+0%")
VOLUME = os.environ.get("TTS_VOLUME", "+0%")
PITCH = os.environ.get("TTS_PITCH", "+0Hz")

# 常用中文音色，换的时候改 TTS_VOICE 即可
VOICES = {
    "xiaoxiao": "zh-CN-XiaoxiaoNeural",   # 女声，温柔自然（默认）
    "xiaoyi": "zh-CN-XiaoyiNeural",       # 女声，活泼
    "yunxi": "zh-CN-YunxiNeural",         # 男声，活泼
    "yunyang": "zh-CN-YunyangNeural",     # 男声，播音腔
    "xiaobei": "zh-CN-liaoning-XiaobeiNeural",  # 女声，东北口音
}

_cache = {}
_cache_lock = threading.Lock()
CACHE_MAX = 64


async def _fetch_mp3(text):
    """把文本合成成 MP3 字节（edge-tts 是异步接口）。"""
    import edge_tts
    buf = bytearray()
    comm = edge_tts.Communicate(text, VOICE, rate=RATE, volume=VOLUME, pitch=PITCH)
    async for chunk in comm.stream():
        if chunk.get("type") == "audio":
            buf += chunk["data"]
    return bytes(buf)


def _decode_to_pcm16(mp3):
    """MP3 → 16k 单声道 int16 PCM。用 PyAV，无需外部 ffmpeg。"""
    import av
    import numpy as np

    pieces = []
    with av.open(io.BytesIO(mp3)) as container:
        stream = container.streams.audio[0]
        resampler = av.AudioResampler(format="s16", layout="mono", rate=TARGET_SR)
        for frame in container.decode(stream):
            for out in resampler.resample(frame):
                pieces.append(np.asarray(out.to_ndarray()).reshape(-1))
        for out in resampler.resample(None):      # 冲刷重采样器里剩余的样本
            pieces.append(np.asarray(out.to_ndarray()).reshape(-1))
    if not pieces:
        return b""
    pcm = np.concatenate(pieces).astype(np.int16, copy=False)
    return pcm.tobytes()


def _cache_put(key, val):
    with _cache_lock:
        if len(_cache) >= CACHE_MAX:
            _cache.pop(next(iter(_cache)), None)
        _cache[key] = val


def synth(text):
    """合成一段语音；失败时抛异常（由服务端回退到提示音）。"""
    text = (text or "").strip()
    if not text:
        return b""
    key = hashlib.md5(("|".join((VOICE, RATE, PITCH, text))).encode("utf-8")).hexdigest()
    with _cache_lock:
        cached = _cache.get(key)
    if cached is not None:
        return cached

    # 播放线程里没有 asyncio 循环，直接 run 一个临时的即可
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        mp3 = asyncio.run(_fetch_mp3(text))
    else:
        # 万一被从有循环的地方调用，另起线程跑，避免破坏当前循环
        box = {}

        def _run():
            try:
                box["mp3"] = asyncio.run(_fetch_mp3(text))
            except Exception as e:  # noqa: BLE001
                box["err"] = e
        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(20)
        if "err" in box:
            raise box["err"]
        mp3 = box.get("mp3", b"")

    pcm = _decode_to_pcm16(mp3)
    if pcm:
        _cache_put(key, pcm)
    return pcm


def list_voices():
    """列出 edge-tts 当前可用的中文音色（调试用，需要联网）。"""
    import asyncio

    async def _go():
        import edge_tts
        vs = await edge_tts.list_voices()
        return [v["ShortName"] for v in vs if str(v.get("Locale", "")).startswith("zh-")]

    return asyncio.run(_go())


if __name__ == "__main__":
    import sys
    txt = sys.argv[1] if len(sys.argv) > 1 else "你好，我是机器狗"
    try:
        data = synth(txt)
    except Exception as e:  # noqa: BLE001
        print(f"合成失败：{e}")
        raise SystemExit(1)
    dur = len(data) / 2 / TARGET_SR
    print(f"音色={VOICE}  文本={txt!r}")
    print(f"输出 {len(data)} 字节 PCM，{TARGET_SR}Hz 单声道，时长 {dur:.2f}s")
    if len(sys.argv) > 2:
        import wave
        with wave.open(sys.argv[2], "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(TARGET_SR)
            w.writeframes(data)
        print("已写出 WAV：", sys.argv[2])
