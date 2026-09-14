# codex_phone / servev1 项目提示词（分支开发用）

> 把本文件全文作为上下文喂给 AI 助手（或作为 Copilot/Codex 的项目说明），即可在不出示历史对话的情况下无缝接手开发。最后更新：2026-09-14。

## 1. 项目定位

ESP32 语音采集端 + PC 识别服务器的**纯 ASR（语音转文字）系统**，是机器狗项目的语音输入模块：

```
ESP32 采集端（INMP441 麦克风，按键录音）
   │  WiFi / WebSocket
   ▼
servev1.py（PC 服务器，Windows）
   │  OPUS 解码 → 16k PCM → SenseVoice 识别
   ▼
文本 ──> 回传板子 LCD 显示（stt）
     ──> 浏览器监测台（:8080）
     ──> 可选：粘贴到 PC 焦点窗口（--pc-input 语音输入模式）
```

明确不做：唤醒词、LLM 对话、TTS。采集由板子按键触发（不是"你好小智"）。

## 2. 文件地图

| 文件 | 职责 |
|---|---|
| `servev1.py` | 全部服务端逻辑（单文件，约 900 行） |
| `test_client.py` | 协议回归测试：模拟采集端走完整协议链 |
| `index.html` | 监测台 GUI（由服务端 :8080 直接托管） |
| `opus.dll` | OPUS 解码依赖（Windows），`opuslib_next` 需要它 |
| `servev1.bat` | 启动脚本（需先 `conda activate dog_voice`） |
| `XIAOZHI.md` | xiaozhi 协议与固件对接细节 |
| `requirements.txt` | Python 依赖 |

## 3. 架构（servev1.py 内部）

**1 个 asyncio 主循环 + 2 个 daemon 线程：**
- `asr_worker` 线程：消费 `asr_queue`（`queue.Queue`）做阻塞识别，绝不把模型调用放进协程；
- `serve_http` 线程：`:8080` 托管 index.html；
- 主循环：`:8765` WebSocket。

**三类客户端，按 URL `?role=` 区分（同端口）：**
- `?role=mic`：FJ-mic 自研采集端，裸 PCM 流（16k/int16/单声道），靠 300ms 静默切段；
- `?role=xiaozhi`：小智协议采集端，OPUS 二进制 + JSON 控制（主力链路）；
- `?role=dash`：浏览器监测台，只收广播事件。

**统一汇合点：** 不管哪条链路，音频最终都变成 PCM bytes 进 `asr_queue`，识别结果双路分发（`broadcast_dash` + 回发 `stt` 给板子）。

## 4. xiaozhi 协议（字节级，勿凭记忆改）

1. 板子连上后发**文本帧** hello：
   `{"type":"hello","version":3,"transport":"websocket","audio_params":{"format":"opus","sample_rate":16000,"channels":1,"frame_duration":60}}`
2. 服务端必须回 hello（否则板子状态机永远拒绝 StartListening，反复断连）：
   `{"type":"hello","version":1,"transport":"websocket","session_id":"<uuid>","audio_params":{"format":"opus","sample_rate":16000,"channels":1,"frame_duration":60}}`
3. 板子发 `{"type":"listen","state":"start"}` → 开始收二进制帧；
4. **每个二进制消息 = 一个 OPUS 帧**（60ms，960 采样）。帧头由 hello 的 `version` 决定：
   - `version=3`：4 字节头 `struct.pack(">BBH", type, reserved, payload_len)`（type=0 音频）；
   - `version=2`：16 字节头（version u16 + type u16 + reserved u32 + timestamp u32 + payload_size u32）；
   - `version=0/1`：**无帧头，裸 OPUS**。
5. 板子发 `{"type":"listen","state":"stop"}` → 整段缓冲提交识别；
6. 识别完回发：`{"type":"stt","text":"...","session_id":"..."}` → 板子 LCD 显示。

**解码：** `opuslib_next.Decoder(16000, 1).decode(payload, 960)`。

## 5. ASR 引擎

- 默认 `funasr`（SenseVoice `iic/SenseVoiceSmall`，中文强、约 1GB、首次从 ModelScope 下载后走本地缓存 `~/.cache/modelscope`）；依赖 torch（CPU 版）；
- 备选 `--engine whisper --model models/faster-whisper-base`（CTranslate2，不依赖 torch）；
- SenseVoice 输出带内部标记（`<|zh|><|NEUTRAL|>...`），已用正则 `re.sub(r'<\|[^|>]*\|>', '', text)` 兜底清洗（官方 `rich_transcription_postprocess` 在 funasr 1.4.15 导入失败被静默吞，别只依赖它）。

## 6. 运行环境（关键，换机器必读）

- **conda 环境 `dog_voice`**：`F:/Users/lenovo/miniconda3/envs/dog_voice`，已装 funasr/torch/faster-whisper/opuslib_next/websockets；
- **opus.dll 加载机制**：`servev1.py` 导入 opuslib_next 前把**脚本自身目录**注入本进程 PATH（只影响本进程，不动系统）。opus.dll 已在仓库里；
- 启动：`conda activate dog_voice && python servev1.py`（或 `servev1.bat`）；
- 板子 URL：`ws://<PC局域网IP>:8765?role=xiaozhi`（PC 用热点共享时通常是 192.168.137.1）。

## 7. 回归测试（改完必跑）

```bash
conda activate dog_voice
python servev1.py &          # 或已有实例在跑
python test_client.py --wav 某段16k单声道说话录音.wav
```

预期输出：`hello 应答 OK → N 帧已发送 → 收到 stt: '...' → PASS`。
注意 `test_client.py` 用 `websockets.connect(url, proxy=None)` 强制直连——本机若开着代理软件（HTTP_PROXY 环境变量），WS 会被代理拦成 502。

## 8. 已踩过的坑（改代码时绕开）

1. **JSON 解析失败静默吞**：`xz_handle_text` 里 `json.loads` 失败直接 return 不留痕——新消息类型分发遗漏时毫无征兆。已加 `[xz-dbg]` 原始消息日志（每连接前 30 条），排查协议问题先看它；
2. **ctypes 句柄截断**：调 `GlobalAlloc/GlobalLock/SetClipboardData` 必须 `restype=ctypes.c_void_p`，否则 64 位句柄截断成 int → access violation；
3. **SendInput 结构体**：Win64 的 `INPUT` 必须凑够 40 字节（union 部分 8 字节 pad）；
4. **git 操作用 `git -C <绝对路径>`**：中文路径 + 本 shell 的 cd 偶发失效，曾在 rebase 时把 `.git` 目录弄丢（工作区无损，重建仓库即可恢复）；
5. **推送通道**：本机代理对 github.com HTTPS 不通，对 ssh.github.com:443 放行。`~/.ssh/config` 已配 `Host github.com → HostName ssh.github.com Port 443`，专用免密钥匙 `id_ed25519_github`。

## 9. 分支开发规范

- 分支命名：`feat/<功能>` / `fix/<问题>` / `exp/<实验>`，从 `main` 拉；
- **改 `servev1.py` 后必须跑第 7 节回归测试**，PASS 才允许合并；
- 版本演进建议沿用 `servevN.py` 命名约定，或以 git tag 标记（`v1.x`）；
- 提交信息用中文，一行主题 + 空行 + 要点列表（参考 `git log`）；
- 推送：`git -C "F:/project/yanjiushenglunwen/yanjiusheng/机器狗项目/server" push origin <分支>`。

## 10. 预定分支方向（按需取用）

- `feat/stream-asr`：流式识别（边说边显字）——把"listen stop 才提交"改成逐帧送 STREAM 型 ASR，服务端补 `is_final` 标记；
- `feat/multi-board`：多板并发接入（`xz_state` 已按连接隔离，识别线程天然串行，主要是测试覆盖）；
- `feat/pc-input-gui`：候选词窗口 / 软件设置面板（当前 `--pc-input` 无 UI）；
- `feat/vad-auto`：免按键模式（服务端 Silero VAD 自动切段，注意环境音误触发）。
