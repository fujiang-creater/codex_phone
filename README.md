> **servev1 打包版**：本目录是独立快照（原 dog_cmd/voice_pc_server），入口脚本 `servev1.py`，启动用 `servev1.bat`（需 dog_voice 环境）。

# 机器狗语音识别服务器（PC 端，仅 ASR）

本阶段 PC 只负责**接收麦克风音频 → 中文语音识别 → 显示原文**，不含控制端转发。

## 链路

```
INMP441 ── ESP32(麦克风端) ──WS 二进制 PCM──> PC 服务器(faster-whisper) ──> 中文原文
                                                      │
                                                      └──WS JSON──> 浏览器监测 GUI(电平/按钮/原文)
```

## PC 服务器行为（按需求规格实现）

1. **WebSocket 服务**：监听可配置端口（默认 `:8765`）。设备连 `ws://<PC_IP>:8765?role=mic`：
   - 从 URL 读取 `role=mic`，识别为麦克风设备；
   - 直接接收二进制消息，**不要求文本握手、不下发开始/停止采集命令**；
   - 浏览器监测台连 `?role=dash`，接收 JSON 状态（见下）。
2. **PCM 协议**：纯 PCM 二进制 —— 16000Hz、有符号 int16、小端、单声道、无压缩、无 WAV 头、无 JSON。
   设备约每 20ms 发一帧（320 采样 / 640 字节）。服务器把**所有**二进制消息拼接成连续 PCM 字节流，
   解析时每 2 字节 = 1 个 int16 采样点，每 320 采样 = 20ms。**不按 WS 消息边界切分，也不当字符串处理**。
3. **按键录音段**：设备只在按住 GPIO9 时发 PCM。
   - 收到 PCM → 按钮按下，新建/追加语音缓冲；
   - 连续约 **300ms** 无 PCM → 判定按钮松开 → **整段提交一次识别**（不会每帧识别）；
   - 维护：当前缓冲、最后收 PCM 时间、是否录音中、是否已提交。
   - 断开连接：关闭连接，当前未完成语音段**丢弃**，等重连后开始新段。
4. **语音转文字**：PCM → 16kHz 单声道 float32（float = int16/32768.0）→ faster-whisper(中文) → 原文。
   PCM 只能还原采样，文字必须靠 ASR 模型。

## 运行

```bash
cd voice_pc_server
pip install -r requirements.txt          # 安装依赖
python servev1.py --model models/faster-whisper-base   # 浏览器开 http://localhost:8080/
```

> 本项目已在 `models/faster-whisper-base/` 预置好 base 模型文件（约 140MB），无需联网下载，直接按上方路径启动即可。

命令行参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--port` | 8765 | WebSocket 端口 |
| `--http-port` | 8080 | 监测 GUI 的 HTTP 端口 |
| `--model` | base | faster-whisper 模型（tiny/small 更快，large 更准） |
| `--device` | cpu | cpu / cuda |
| `--compute` | int8 | 计算精度 int8 / float16 |
| `--gap` | 0.30 | 按钮松开判定间隔（秒） |

## 监测 GUI

打开 `http://localhost:8080/`，实时显示：

- 麦克风连接状态、服务器连接状态；
- **实时 VU 电平**、按钮按下/松开、是否录音中、当前缓冲毫秒数；
- 最近一次识别原文 + 该段波形；
- 识别历史（时间戳 / 音频时长 / 识别耗时）、事件日志。

## 固件（麦克风端）

`../FJ-mic` 工程：INMP441 采集（24bit→16bit），按住 GPIO9 时每 20ms 经 WS 上传一帧 640 字节 PCM。
`idf.py set-target esp32s3 && build && flash`；`main/main.c` 顶部填 `WIFI_SSID/PASS` 与
`VOICE_PC_WS_URL=ws://<PC_IP>:8765?role=mic`，三设备同 Wi-Fi。

## 识别精度优化

若识别结果出现乱码、漏字、同音错字，按优先级检查：

1. **换更大的模型**：`tiny` 速度最快但中文准确率最低，`base` 明显好一个档次，`small` 更稳。本项目已预置 `base`，启动时 `--model models/faster-whisper-base` 即可。
2. **看 GUI VU 电平**：按住 GPIO9 说话，右上角电平条应在 0.3~0.8 之间跳动。电平过低（<0.1）说明麦克风增益/距离不够；电平顶满（>0.95）说明削波，都会让识别变差。
3. **保持 15~30cm 正对麦克风**：太远或偏轴，信噪比下降。
4. **确认 I2S 采样率严格 16kHz**：ESP32 端 I2S 时钟若偏差，声音会变调，whisper 会识别成奇怪内容。
5. **环境噪音**：安静房间 > 有风扇/人声背景；`vad_filter=True` 会尝试过滤静音，但噪音过大会被当成语音。

## 本阶段不做（后续扩展）

- 控制端 `role=ctrl`、action/move/stop 转发、TTS 发声、向控制端发音频；
- 离线 VAD 端点检测（当前用 300ms 静音间隙切段，已够用）。

---

## 适配 xiaozhi（小智 / LinkDog）采集端

`servev1.py` 现在**两种采集端都能接**：原来的 `FJ-mic`（裸 PCM，`?role=mic`）和 xiaozhi 板子（OPUS 二进制协议，`?role=xiaozhi` 或连上来自动识别）。xiaozhi 那套协议、握手、OPUS 解码和"回显文字到板子 LCD"是怎么做的，以及板子 `config.yaml` 要怎么填，详见 **[XIAOZHI.md](./XIAOZHI.md)**。

要点：xiaozhi 板子把麦克风编码成 OPUS 上传，`servev1.py` 在中间解码回 16k PCM 后**完全复用你原有的识别/降噪/浏览器监测台代码**；识别完会回发 `{"type":"stt","text":...}` 让板子 LCD 直接显示文字，不经过 LLM/TTS。

## PC 语音输入模式（--pc-input）

把本服务当作**电脑的语音输入模块**：板子按键说话 → 识别 → 文字自动粘贴到当前焦点窗口的光标处（微信、Word、浏览器输入框等都行）。

```bash
python servev1.py --pc-input
```

- 实现：识别文本写入剪贴板 + 模拟 `Ctrl+V`（纯 ctypes，Windows 专用，无需装包）；
- 注意：会覆盖剪贴板内容；不加此参数时行为与普通识别服务器完全一致。

## 协议回归测试

```bash
python test_client.py --wav 某段16k单声道说话录音.wav
```

模拟 xiaozhi 采集端走完整协议（hello → listen start → OPUS 帧 → listen stop → 等待 stt 回传），用于改服务端代码后的自测。
