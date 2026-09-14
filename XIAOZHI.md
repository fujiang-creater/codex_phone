> **servev1 打包版**：本目录是独立快照（原 dog_cmd/voice_pc_server），入口脚本 `servev1.py`，启动用 `servev1.bat`（需 dog_voice 环境）。

# 让本服务端适配 xiaozhi（小智 / LinkDog）采集端

> 目标：把你现有的 `servev1.py`（原本只认 `FJ-mic` 发的**裸 PCM**）改造成**也能认 xiaozhi 采集板发的 OPUS 二进制协议**的服务端。
> 改完后，两种采集端可以同时连同一个服务端：浏览器监测台统一看结果；xiaozhi 板子的 LCD 还会直接显示识别出的文字。

---

## 1. xiaozhi 服务端是怎么工作的（先搞懂它）

xiaozhi 官方服务端（`xiaozhi-esp32-server`）和你这个 `servev1.py` 干的是同一件事——**收音频、跑 ASR、把文字回显**——但协议差很多。它是一条**二进制协议 + 文本 JSON 指令**混合的 WebSocket，分四步：

### ① 握手（hello）
板子一连上就先发一段文本 JSON 自我介绍：

```json
{"type":"hello","version":3,
 "audio_params":{"format":"opus","sample_rate":16000,"channels":1,"frame_duration":60},
 "features":{"mcp":true}}
```

- `version`：协议版本。**2 → BinaryProtocol2（16 字节帧头）；3 → BinaryProtocol3（4 字节帧头）；0/1（默认）→ 裸 OPUS 无帧头**。
- `audio_params.format = "opus"`：板子发来的音频是 **OPUS 压缩**的，不是 PCM。采样率 16k、单声道、每帧 60ms。
- 服务端必须**回一段 hello**，里面带 `session_id` 和 `audio_params`，板子收到后才认为"音频通道已打开"。

### ② 开始/结束说话（listen）
板子检测到唤醒词（或你按了按键）后发：

```json
{"type":"listen","state":"start"}     // 开始上传音频
{"type":"listen","state":"stop"}      // 说完，停止上传
```

注意：**句子边界由板子用 start/stop 包出来**，服务端不用自己靠"静音 300ms"去猜（这点比你的 FJ-mic 链路更干净）。

### ③ 音频帧（二进制 OPUS）
`listen start` 之后，板子开始发**二进制消息**，每帧 = 帧头（看 version）+ 一段 OPUS 数据。服务端把每帧 OPUS 解回 16k/int16/mono 的 PCM，拼起来就是一段完整语音。

服务端解码用的是 `opuslib_next.Decoder(16000, 1).decode(pkt, 960)`（960 = 60ms × 16k 采样）。

### ④ 回显文字（stt）
识别完成后，服务端把文字发回板子：

```json
{"type":"stt","text":"前进两步"}
```

板子 `application.cc` 收到 `stt` 后直接 `SetChatMessage("user", text)` —— **就是显示在它 LCD 上的那行字**。这一步正是"把说的话转成文字显示出来"的落点，且不需要 LLM / TTS。

> 官方完整链路里服务端还会接着调 LLM、再发 `tts` 语音回来。我们**只取"收音频 → 识别 → 回 stt"这一段**，LLM/TTS 全部不要，正好对应你论文里"纯语音转文字显示"的需求。

---

## 2. 本适配器在 `servev1.py` 里改了什么

| 位置 | 改动 | 作用 |
|---|---|---|
| 顶部 import | 新增 `opuslib_next`（try 包一层，没装也不崩）+ `XZ_*` 常量 | OPUS 解码依赖与帧参数 |
| `clients` / `xz_state` | 新增 `"xz"` 角色池与 `xz_state` 会话表 | 管理 xiaozhi 连接 |
| `xz_strip_header()` | 按 `version` 剥 16B / 4B / 0B 帧头 | 拿到纯 OPUS 载荷 |
| `xz_decode_opus()` | `opuslib_next` 解 OPUS → PCM | 喂进你原有的识别管线 |
| `xz_handle_hello/text/binary()` | 握手应答、listen 分段、收音频解码 | 协议主逻辑 |
| `submit_buffer()` | 抽出"整段提交识别"（FJ-mic 与 xiaozhi 共用） | 避免两段重复代码 |
| `asr_worker()` | 识别完若来自 xiaozhi，回发 `stt` | 板子 LCD 显示 |
| `handler()` | 新增 `?role=xiaozhi`；无 role 时收到 hello 自动升级 | 双采集端共存 |
| 启动检查 | 没装 `opuslib_next` 时打印明确安装提示 | 别懵 |

**关键兼容点**：xiaozhi 板子发的是 OPUS，你原来 `transcribe()` 要的是 16k/int16 PCM——适配器在中间把 OPUS 解回 PCM，后面识别、降噪、波形、浏览器监测台**全部复用你原来的代码**，一行没动。

---

## 3. 板子那边要怎么配（固件侧，一次性）

只改 xiaozhi 工程里板子的 `config.yaml`（或编译时的 `menuconfig` 对应项），让板子连到你的电脑：

```yaml
wifi:
  ssid: "你的WiFi"
  password: "密码"
websocket:
  url: "ws://192.168.x.x:8765?role=xiaozhi"   # 换成你电脑局域网 IP，端口=本服务端 PORT(8765)
  token: ""                                    # 留空，本地服务端不校验鉴权
  version: 3                                   # 用 BinaryProtocol3（4 字节帧头），可选 0/1 发裸 OPUS
```

唤醒词相关（按你论文需求二选一）：

- **想要按键录音**（和你 FJ-mic 一样）：编译时开 `CONFIG_USE_NO_WAKE_WORD=y`，并把板子 BOOT 键接到 `ToggleChatState`（LinkDog 默认已接，按一下开始/停止听）。这样不会一直监听"你好小智"。
- **保留唤醒词**：什么都不用改，说"你好小智"即开始，说完服务端回显文字。

> 板子和服务端**必须同一局域网**。IP 不要写 `localhost`/`127.0.0.1`（那是板子自己）。

---

## 4. 运行

```bash
cd voice_pc_server
pip install -r requirements.txt        # 含 opuslib_next（opus.dll 已随项目自带，无需另装）
python servev1.py --model models/faster-whisper-base
```

- 浏览器开 `http://localhost:8080/` —— 和原来一样看电平 / 识别原文 / 历史。
- xiaozhi 板子按 BOOT（或说"你好小智"）→ 说话 → LCD 显示文字，同时浏览器也出现识别结果。
- 日志里应看到：`[xz] hello: version=3 ...` 和 `[xz] 开始说话（listen start）` / `提交识别`。

---

## 5. 排错

- **日志提示 `未安装 opuslib_next`**：`pip install opuslib_next`。Windows 报 "Could not find Opus library" 是缺 `opus.dll`：本项目目录已自带 `opus.dll`（取自 conda-forge libopus 官方包），`servev1.py` 启动时会自动把所在目录注入本进程 PATH 来加载它——**不需要**再改系统 PATH 或 conda 装 opus。若换目录运行，把 `opus.dll` 一起拷走即可。
- **板子连上但一直不识别**：确认 `websocket.url` 的 IP 和端口对；确认板子发了 `listen start`（日志有 `[xz] 开始说话`）。若板子走的是 `version: 0/1` 裸 OPUS，适配器也能解（自动不剥头）。
- **LCD 显示了但浏览器没有 / 反过来**：监测台和板子是两个独立的 WS 连接，互不影响；任一侧没收到都先查那一侧的连接日志。
- **板子说话后卡在"聆听"不回到待机**：因为我们只回 `stt`、不发 `tts`。再按一次 BOOT 即可停止；若要板子自动复位，可在 `xz_handle_text` 的 `listen stop` 后补发一条 `{"type":"tts","state":"stop"}`（可选，不影响识别）。
