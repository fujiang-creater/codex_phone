# -*- coding: utf-8 -*-
"""servev1 协议回归测试：模拟 xiaozhi 采集端完整流程。
hello -> listen start -> BP3 OPUS 帧 -> listen stop -> 等待 stt 回传。

用法：
    python test_client.py --wav 说话录音.wav          # 语音识别测试（需 16k/mono/int16）
    python test_client.py --url ws://192.168.1.5:8765/?role=xiaozhi
"""
import argparse
import asyncio, json, os, struct, sys, time
import wave

import numpy as np

os.environ["PATH"] = os.path.dirname(os.path.abspath(__file__)) + os.pathsep + os.environ.get("PATH", "")
import opuslib_next
import websockets

URL = "ws://127.0.0.1:8765/?role=xiaozhi"
SR, CH, FRAME_MS = 16000, 1, 60
FRAME = SR * FRAME_MS // 1000  # 960 samples


def load_pcm(wav_path):
    with wave.open(wav_path, "rb") as w:
        assert w.getframerate() == SR and w.getnchannels() == 1 and w.getsampwidth() == 2, \
            f"unexpected fmt: {w.getframerate()}/{w.getnchannels()}/{w.getsampwidth()}"
        return w.readframes(w.getnframes())


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wav", required=True, help="16k/mono/int16 说话录音 WAV")
    ap.add_argument("--url", default=URL)
    a = ap.parse_args()
    pcm = load_pcm(a.wav)
    n_frames = len(pcm) // (FRAME * 2)
    print(f"[test] WAV {len(pcm)/2/SR:.2f}s -> {n_frames} OPUS frames")

    enc = opuslib_next.Encoder(SR, CH, "voip")
    # proxy=None：强制直连，避免系统代理拦截 ws://127.0.0.1
    async with websockets.connect(a.url, proxy=None) as ws:
        # 1) hello
        await ws.send(json.dumps({
            "type": "hello", "version": 3, "transport": "websocket",
            "audio_params": {"format": "opus", "sample_rate": SR,
                             "channels": CH, "frame_duration": FRAME_MS},
        }))
        hello_back = json.loads(await ws.recv())
        assert hello_back.get("type") == "hello" and hello_back.get("session_id"), hello_back
        print(f"[test] hello 应答 OK, session_id={hello_back['session_id'][:8]}...")

        # 2) listen start
        await ws.send(json.dumps({"type": "listen", "state": "start", "mode": "manual"}))

        # 3) 二进制 OPUS 帧（BP3 头）
        t0 = time.time()
        for i in range(n_frames):
            chunk = pcm[i * FRAME * 2:(i + 1) * FRAME * 2]
            op = enc.encode(chunk, FRAME)
            await ws.send(struct.pack(">BBH", 0, 0, len(op)) + op)
            await asyncio.sleep(0.02)  # 约三倍速发送，缩短测试时间
        print(f"[test] {n_frames} 帧已发送（{time.time()-t0:.1f}s）")

        # 4) listen stop
        await ws.send(json.dumps({"type": "listen", "state": "stop"}))

        # 5) 等 stt（服务端识别一般 <1s，给 30s 余量）
        t0 = time.time()
        while time.time() - t0 < 30:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=30)
            except asyncio.TimeoutError:
                break
            if isinstance(msg, str):
                j = json.loads(msg)
                if j.get("type") == "stt":
                    print(f"[test] 收到 stt: {j.get('text')!r}（{time.time()-t0:.2f}s）")
                    print("[test] === 端到端跑通 PASS ===")
                    return
                else:
                    print(f"[test] 其他消息: {j.get('type')}")
        print("[test] === 未收到 stt，FAIL ===")
        sys.exit(1)


asyncio.run(main())
