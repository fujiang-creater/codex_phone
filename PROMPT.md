# 机器狗项目 · AI 交接提示词（agent 版）

> **用法**：把本文件**全文**作为上下文喂给 AI agent（Claude / Codex / CodeBuddy 等），不需要附任何历史对话。
> 本文件是**唯一事实来源**；若与 `README.md`、`XIAOZHI.md` 冲突，**以本文件为准**（那两份是 09-14 及更早的旧文档）。
> 最后更新：**2026-09-21**。上一版（2026-09-14）描述的是"纯 ASR、无 LLM/TTS、GUI 在 :8080、servev1.py 约 900 行"的阶段，**整份作废**。

---

## 0. 给 agent 的执行约定（先读这段）

1. 本文件里标注 **"已实现 / 真机验证"** 的，都是 2026-09-20 在真板上跑通过的；标注 **"未修 / 待办"** 的，**不要当成已完成**。
2. **路径不要写死。** 项目 2026-09-21 已从 Windows 迁到 **Ubuntu**，旧 `F:\...`、`\\wsl.localhost\...` 全部失效。
   用**仓库身份**定位（服务端 remote 含 `codex_phone`，板端含 `FJ.git`）：
   ```bash
   find ~ /opt /srv /data /mnt -maxdepth 6 -name .git -type d 2>/dev/null |
     while read g; do d="$(dirname "$g")"; echo "$d  ->  $(git -C "$d" remote get-url origin 2>/dev/null)"; done
   ```
   项目根还带了一个现成脚本：`bash locate-project.sh`（只读，会打印真实路径 + 环境体检）。
3. **不要改板端仓库**（理由见 §9）。本项目的既定策略是：**服务端单边消化两端差异**。
4. 改动 `servev1.py` 后**必须**跑 §11 的自测脚本，PASS 才算完成。
5. 涉及**字节级协议**的改动，**先去读代码**，不要凭记忆写。

## 1. 硬约束（违反即错）

- ❌ 不得提交 `llm_config.json`（**含真实大模型 API 密钥**）、`*.bin`、`twin_store.json`、`_test_*.py`、`.server_*.log` —— 均已在 `.gitignore`。
- ❌ 不得修改板端固件代码，也不得要求用户"改板子再重烧"来配合服务端改动。
- ❌ 不得把 `sequence.run`（服务端位置序列）与 `action.run`（控制板预存动作组）**合并成一套数据源**。
- ❌ 不要为了跑起来就 `pip install` 到系统 Python；一律用 venv / conda（见 §4）。
- ✅ 服务端要能同时在 Windows 与 Linux 运行。Windows 专有代码（剪贴板、PC 键盘输入）**全部在 `if os.name == "nt"` 分支内**，新增代码也必须这样隔离。
- ✅ 提交信息用中文：一行主题 + 空行 + 要点列表。

## 2. 项目是什么（当前形态）

ESP32-S3 四足机器狗（16MB flash / 8MB PSRAM）+ PC 端 Python 服务中心。

```
┌─ 板端 (ESP-IDF v6.0.2) ────────────┐        ┌─ PC 服务端 servev1.py ──────────────┐
│ 按键/唤醒 → I2S 麦克风采集          │        │ :8765  WebSocket（角色见 §6）        │
│  Opus 编码 16k/60ms  ──── WS ─────► │ ─────► │  解码 → SenseVoiceSmall ASR（中文）  │
│  I2S 喇叭播放 Opus   ◄── WS ─────  │ ◄───── │  → glm-4-flash LLM → edge-tts 合成   │
│  TFT 表情（10 种，代码绘制）         │        │ :8000  HTTP 控制台 5 个页面 + 25 个 /api │
│  12 路舵机 / 动作组 / OTA 服务       │        │  设备状态机 · 数字孪生 · OTA 编排      │
└────────────────────────────────────┘        └──────────────────────────────────────┘
```

**语音链路三方**（改这块前先记住）：

| 环节 | 实现 | 位置 | 备注 |
|---|---|---|---|
| 语音→中文 | `iic/SenseVoiceSmall` + `fsmn_vad_zh-cn-16k-common-pytorch`（FunASR，**CPU**） | `servev1.py: load_asr()` :203 / `transcribe()` :253 | 首次从 ModelScope 下载到 `~/.cache/modelscope`；输出自带 `<\|zh\|><\|NEUTRAL\|>` 标签，已用正则清洗 |
| 中文→回复 | `glm-4-flash`（智谱） | `brain.py` + `llm_config.json` | 调试台 `/brain` 可改配置 |
| 回复→语音 | `edge-tts` 的 `zh-CN-XiaoxiaoNeural`（**在线**） | `tts_edge.py::synth()` | 项目自带实现，**不依赖 pip 的 edge-tts 包**；输出 24kHz MP3，由 PyAV 解码 + `_resample_pcm()` :2153 重采样到 16kHz |

## 3. 仓库与代码位置

| | 服务端 | 板端固件 |
|---|---|---|
| 仓库根 | **就是 `server/` 目录本身** | 独立仓库，含 `main/` + `partitions_ota.csv` |
| remote | `codex_phone` | `FJ.git` |
| 分支 / HEAD | `main` @ `abf6598` | `2413698` |
| 语言/工具 | Python 3.11（单文件服务） | C / ESP-IDF v6.0.2 |
| AI 可否直接编译 | — | **可以**（Ubuntu 下 `idf.py build`；旧"WSL 被安全策略拦"的限制已不成立） |

- ⚠ 远端残留分支 **`dev-v2`**（09-16，比 `main` 少 8 个提交，多一个 1422 行旧版 `servev1.py` + `opus.dll`）——**误 merge / checkout 会冲掉现有改动**，待清理。
- ⚠ **别改错目录**：同级的 `server_v2/` 是上面那个 `dev-v2` 的本地旧副本；`esp32s3n16r8-control/` 是**早期控制骨架**（无 `ota_service.c`、无分区表）。
  **判真口诀：有 `main/ota_service.c` 且仓库根有 `partitions_ota.csv`，才是真板端固件。**

## 4. 环境与启动

```bash
# ① 环境（原 conda env dog_voice 已被删除；按精确清单重建，Python 3.11）
cd server
python3.11 -m venv .venv && . .venv/bin/activate
pip install -U pip
pip install --extra-index-url https://download.pytorch.org/whl/cpu -r requirements.lock.txt
#   · 120 个包，2026-09-21 从旧机 pip 缓存逐包还原出来的精确版本
#   · Windows 另需 requirements.windows-extra.txt（仅 pywin32），Linux 不要装
#   · 旧机 pip 缓存里的 wheel 是 win_amd64/cp311，**Linux 用不了，必须重下**；
#     唯一可跨平台复用的是模型缓存 ~/.cache/modelscope（901MB，含上面两个模型）→ 整目录拷过来即可免下载

# ② 启动完整服务
python -u servev1.py              # 大模型对话：llm_config.json 配好 key 即**自动启用**
                                  #   强制开/关才用 DOG_BRAIN=1 / DOG_BRAIN=0

# ③ 无 torch 机器的降级模式（只看/调前端页面时用）
python -u servev1.py --ctrl-only  # :8000 五页正常，但**不起 :8765** → 板子连不上、OTA 下发不通
```

- 端口：**8000** = HTTP 控制台 + `/api`；**8765** = WebSocket（**板子连的就是这里**）。
- **启动约 1.5~3 分钟**（FunASR 加载模型）。**判断就绪看日志 `[server] WebSocket :8765` 那一行，别看 8000 通没通。**
- 设备 IP：板子走 DHCP，**IP 会变**（历史见过 .82 / .249），别在配置里写死；日志里"本机可用 IP"那行会列出全部网卡，固件 URL 必须选**与设备同网段**的那个。
- 控制台鉴权：开发模式 token 填**任意非空字符串**即可；生产用 `--device-tokens`（每个角色一个 token）。

## 5. `servev1.py` 结构地图（3623 行 / 174KB，单文件）

| 行号 | 区域 |
|---|---|
| ~130-200 | 音频工具：RBJ 滤波器组、`denoise_audio()`、`float_to_pcm16()` |
| 203 / 253 / 423 | **ASR**：`load_asr()` / `transcribe()` / `asr_worker()`（阻塞识别放独立线程，**绝不进协程**） |
| 534 / 557 / 681 | WS 鉴权 `_ws_token_from()` / 主 `handler()`（按 `?role=` 分派）/ 监测台 `monitor()` |
| 746-960 | **xiaozhi 协议**：`xz_strip_header()` / `xz_decode_opus()` / `xz_handle_hello()` / `xz_handle_text()` / `xz_handle_binary()` / `submit_buffer()` |
| 974-1062 | HTTP 服务 `serve_http()`（含 `/api` 路由分发） |
| 1025 | `DEV_STATES = ("IDLE","LISTENING","SPEAKING","MOVING","OTA","UPGRADING","ERROR","OFFLINE")` |
| 1143-1500 | **设备状态机**：`dev_set_state()` / `dev_get_state()` / `dev_set_audio()` / `dev_bind_ws()` / `dev_gate()`（指令总闸） |
| 1534-1660 | 下行通道：`ws_command()` / `_schedule_status_probe()`（握手后 1.5s 主动取固件版本） |
| 1658-1900 | 舵机与数字孪生：`servo_dispatch()` / `twin_*`（配合 `twin.py`） |
| 1926-2250 | **TTS 播放链**：`synth_speech()` / `audio_play_text()` / `_play_next()` / `_resample_pcm()` / `_play_worker()` / `play_abort()` |
| **2305** | **OTA**：`ota_absorb_report()`（⚠ 已知 bug，见 §10）→ `ota_on_hello()` / `ota_start()` / `ota_status()` / `ota_finish()` |
| 2601 / 2637 / 2664 | `_lan_ip_for()`（按设备网段挑本机 IP）/ `read_app_desc()`（解析镜像内嵌 `esp_app_desc`）/ `load_firmware_index()` |
| 2771-3476 | `/api/*` 各端点实现（`_ctrl_*` 系列） |
| 3514 / 3529 / 3584 | `main()` / `--ctrl-only` 分支 / 命令行参数 |

配套模块：`brain.py`(521 行，大模型) · `twin.py`(605 行，数字孪生) · `llm_cloud.py` · `tts_edge.py`(146 行) · `mcp_dog_server.py` · `mock_esp32.py` · `dog_actions.json`。

## 6. 两端协议（字节级，改前先读代码）

**上行**（板 → 服务端）只发 4 种：`hello` / `state` / `command_result` / `listen`。
**下行**（服务端 → 板）只认 3 种：`hello` / `command` / `audio_play`。

1. 板端 hello（**version=3**）：
   `{"type":"hello","version":3,"transport":"websocket","audio_params":{"format":"opus","sample_rate":16000,"channels":1,"frame_duration":60}}`
2. 服务端**必须回** hello，否则板端状态机永远拒绝 `StartListening` 并反复断连：
   `{"type":"hello","version":1,"transport":"websocket","session_id":"<uuid>","audio_params":{...}}`
3. `listen:{"state":"start"}` → 收二进制；`{"state":"stop"}` → 提交整段识别。
4. 音频帧：**每个 WS 二进制消息 = 一帧 Opus**（60ms / 960 采样）。帧头由 hello 的 `version` 决定：
   - `version=3`：**4 字节** `struct.pack(">BBH", type, reserved, payload_len)`（type=0 为音频）；
   - `version=2`：16 字节头；`0/1`：裸 Opus 无头。
   - 解码：`opuslib_next.Decoder(16000, 1).decode(payload, 960)`。
5. 角色（同一 :8765 端口，靠 `?role=` 区分）：`xiaozhi`（主链路，板端）· `dog`（设备侧）· `mic`（裸 PCM 采集端）· `dash`（监测台，只收广播）。
6. 下行信封校验在**板端**：`main/command_validation.c:60` 的 `command_validate_envelope()`（由 `command_dispatcher.c:356` 调用）要求 `version == 1`（整数）、`request_id` 与 `command` 均非空、`command` 必须命中板端已知指令表，`mode` 可选 ∈ {`sync`,`async`}（缺省 sync），`args` 可省（缺省时取根对象）。**它不检查 `type` 字段** —— 服务端不要为此多塞字段。
7. WS 心跳 `ping_interval=30 / ping_timeout=60`；角色 token 仅在 `DEVICE_TOKENS` 非空时校验，**`dash` 永不校验**（否则监测台自己连不上）。
8. 三处**曾经静默失效**的脆弱耦合（09-20 已修，别改回去）：
   ① 音频采样率/帧长必须从 hello 的 `audio_params` **协商**（`dev_set_audio/dev_get_audio` + `_resample_pcm` 兜底），不能写死；
   ② **HTTP 直推路径的信封也必须带 `policy`**（原来只有 WS 信封带 → HTTP 推永远排队、打不断当前播放）；
   ③ 固件版本 `fw_version` **板端 hello 和周期 state 都不带**，只能服务端握手后调 `device.status` 取。

## 7. 页面（:8000）

| 路由 | 文件 | 用途 |
|---|---|---|
| `/`（`/portal`） | `portal.html` | 服务台首页 |
| `/ctrl` | `ctrl.html` | 设备控制 / OTA / 指令下发 |
| `/brain` | `brain.html` | 大模型调试台 |
| `/twin` | `twin.html` | 数字孪生主控台 |
| `/voice` | `index.html` | 语音监测台（**文件真名是 `index.html`**，`/voice` 只是路由；原独立 :8080 已并入） |
| `/sdcard` | `sdcard.html` | **SD 卡文件控制台（09-21 新增）**：列目录 / 上传 / 下载 / 改名 / 新建目录 / 删除。由服务端代理转发到板端 `/api/sd/*` |

- 路由表在 `servev1.py:3502-3512`；`_serve_html()` :3170 **实时读盘无缓存** → 改完刷新即可生效。
- ⚠ **页面必须经 http 访问**。用 `file://` 打开时 `href="/x"` 会解析成盘根、`fetch` 全失效。09-20 已把 5 页站内链接改成相对路径，并注入 `file://` 自动跳转脚本——**新增链接请沿用相对路径**。
- ⚠ 本机 curl 探活要加 `--noproxy '*'`：环境里设了 `HTTP_PROXY/HTTPS_PROXY=127.0.0.1:55639`，不加会把请求发给代理、把活着的本地服务误判成 502。

### 7.1 SD 卡通道（服务端已实现，板端待实现）

浏览器**只跟 `:8000` 说话**，服务端按 `DEVICE_ENDPOINTS` 里的板子 IP 转发；文件是**流式透传**（不落盘、不整块缓存）。

| 服务端路由 | 方法 | 转发到板端 |
|---|---|---|
| `/api/v1/sd/devices` | GET | 无（列已知设备 + 各设备 `sd_mounted/sd_total_kb/sd_free_kb`） |
| `/api/v1/sd/info` | GET | `GET /api/sd/info` |
| `/api/v1/sd/list` | GET | `GET /api/sd/list?path=` |
| `/api/v1/sd/stat` | GET | `GET /api/sd/stat?path=`（只答"在不在"，重名校验用） |
| `/api/v1/sd/download` | GET | `GET /api/sd/file?path=`（流式） |
| `/api/v1/sd/upload` | POST | `POST /api/sd/file?path=`（流式，**必须**分块）；确认覆盖时才带 `overwrite=1` |
| `/api/v1/sd/delete` · `/mkdir` · `/rename` | POST | 同名板端路由 |

**上传前的三道预检，全部在读 body 之前**（用户白传几百 MB 是最糟的体验）：

1. **上限**：单文件上限 = 256MB（`SD_MAX_UPLOAD_FALLBACK`）。板端在 `/api/sd/info` 报的
   `max_upload_kb` 是权威值，服务端每轮上传先 `sd_upload_caps()` 探一次：**固件不支持 / 未插卡 /
   不可达都当场拒绝**，不会退回兜底值硬传。超限 → `413 SD_TOO_LARGE`。
2. **剩余空间**：同一个 `info` 调用里的 `free_kb` 用来算放不放得下 → 塞不下 `507 SD_NO_SPACE`。
   8GB 卡 + 256MB 单文件 + 20MHz SPI（分钟级）的组合下这道预检很值。
   ⚠ `_kb()` 必须区分"**字段缺失**"（→ `None`，跳过预检）和"**0**"（卡真的满了，要拦）——
   第一版写成 `free or None` 把两者混为一谈，满卡会被放行。
3. **重名**：① 页面先调 `/api/v1/sd/stat`，目标已存在就弹「覆盖」确认；
   ② `POST /api/v1/sd/upload` 里还有一道**权威门禁**：未带 `overwrite=1` 且同名已存在 →
   `409 SD_EXISTS` + `certain:true` + `existing{type,size}`，**一个字节都不写**；
   ③ 探不出来时（板端有 info 没 stat）**绝不按"不存在"放行** → `409 SD_EXISTS` + `certain:false`，
   让用户显式确认后带 `overwrite=1` 再传。页面据 `certain` 显示不同文案。

- 服务端侧校验：`sd_parse_path()`（拒 `..`、非 ASCII、DOS 保留名、段尾 `.`/`-`）· `sd_write_guard()` 仅在 OTA/UPGRADING 拦写 · `_sd_parse_json()` 把**不带 `SD_` 前缀的 404 一律归一成 `SD_NOT_SUPPORTED`**（旧固件通用 404 会让人误以为是"路径不存在"）。
- ⚠ `sd_put_stream()` 的两个要点（09-22 修）：① 发 body 中途断了**也要尽量把板端应答读下来** —— 板端超限/同名/未插卡都是读完前就返回，不读就只剩一句笼统的"传输中断"；② 但在读之前**必须先对板端半关**（`conn.sock.shutdown(SHUT_WR)` 发 FIN）：否则板子还在等剩余字节，而我们直等 `getresponse()`，**两边互等到 300s 超时**（服务端线程 + 板子 HTTP 线程全占住）。半关后板子立刻走错误分支清掉半截文件，再给 `SD_ABORT_GRACE`（5s）宽限期读它的真实错误码。
- 为什么全走 HTTP、**不新增 WS 命令**：① 指令回执 `result_json[768]` 装不下文件列表；② WS 只收单帧完整文本、`buffer_size` 默认 1024，塞不进文件内容；③ 这样 `command_validation.c` 白名单（19 条）和 `command_dispatcher.c` 分发表**零改动**。
- 板端要提供哪些接口 → 见 `../SD卡控制台-板端接口清单.md`（一页清单：8 条 HTTP 路由 + 3 个 state 字段 + 错误码表，**不含服务端实现说明，也不含板端设计**）；可行性分析 → `../SD卡控制台-可行性评估.md`。
- **8GB 卡**（真机就是这块）：容量用 **KB** 上报别用字节（8GB=8.6e9 已超 uint32）；板端卡**必须 FAT32**（ESP-IDF 的 FATFS `FF_FS_EXFAT=0`，exFAT 挂了只会表现为"没插卡"）。
- ⚠ **板端畸形状态行要单独识别（09-22）**：实测插卡后板端 `/api/sd/info` 的第一行不是 `200 OK` 而是一串内存垃圾，`http.client` 解析时直接抛 `BadStatusLine` → 旧代码落进通用 `except`，报成 `BOARD_UNREACHABLE` + "连不上板子 <IP>:80"，**把"板端 bug"误报成"网络不通"**（排查方向直接被带偏）。现在 `sd_http_json()` 单捕 `BadStatusLine`：先用 `_sd_raw_request()`（裸 socket，按第一个空行切出正文）把正文抠出来，正文里若是有效错误码就**直接透传**（实测拿回真实的 `SD_IO_ERROR: failed to read SD capacity`），否则回新码 `BOARD_BAD_RESPONSE` 并说明"板端该路由把状态行写坏了"。
- ⚠ **页面上"板子 IP"有两个来源，别再混（09-22）**：① `ip` = **板子自报**（`device.status` 回执，`command_dispatcher.c:278` 调 `wifi_sta_get_ip()`）；② `endpoint` = 服务端**真正拿去连板子**的地址（`DEVICE_ENDPOINTS[did].host`，来自 WS 握手源 IP）。`/api/v1/sd/devices` 现在两个都给，另带 `endpoint_auto` / `ip_mismatch` / `endpoint_corrected_from`，页面同时显示并在不一致时标红。板子是 DHCP（`wifi_sta.c` 无静态 IP），**OTA/断电重启后 IP 会漂**（实测 .184 → .213）。
- ⚠ **端点自动纠偏（09-22 修）**：`device_endpoint_register()` 旧逻辑是"手动登记过的记录一律挡住自动更新"，于是板子换 IP 后端点表永远停在旧地址、服务端一直拿旧地址去连。现在自动登记（auto=True，源 IP 实时可信）**仍会纠正 host**，但保留用户填的 port/token，并把旧值记进 `corrected_from`/`corrected_at` + 打一条 `[ctrl] 端点自动纠偏` 日志；`overwrite=True`（控制台手动登记）行为不变，仍是整条替换。
- **SD 台不再是一次性快照（09-22）**：`sdcard.html` 每 3s 轮询 `/api/v1/sd/devices`（`pollDevices()`），只有 `devSig()` 签名变化才重绘；选中设备的**地址或在线状态**一变就 `clearBanner()` + 重取 `info`/`list`。以前既没有轮询、`showBanner()` 也不会清空 → 那句嵌着旧 IP 的"连不上板子 X.X.X.X:80"会一直挂在页面上，看起来就像"页面在乱报板子 IP"。
- 自测与演示：`_test_sd.py`（**92 项，全过**：含容量/空间预检/断流不挂死/多分块一致性）· `_demo_sd.py`（假板子 + 真 ControlHandler，**端口由内核分配**，按 8GB 卡报容量，启动时打印 `/sdcard` 地址，无需硬件）。两者均被 `_test_*.py`/`_demo_*.py` 规则 gitignore。

## 8. OTA 流程（app 分区热更新，已真机验证）

1. 固件准备（**手工**，非联网下载）：板端构建产物 `build/esp32s3n16r8_idf.bin`
   → 拷贝为 `firmware/esp32s3/<版本>/app.bin`（即 `flasher_args.json` 中 offset `0x20000` 的 app 条目）。
   **入库后不用再重启服务**：调 `GET|POST /api/v1/firmware/reload` 即可重扫 `firmware/` 换表（09-22 加）。
   注意索引只在启动时算一次，**不调 reload 就还是旧表**。`*.bin` 已 gitignore（clone 拿不到）。
   当前库内：`1.0.1`（1,339,120 B，09-19 版，**无 SD**）· `1.0.2`（**1,412,512 B**，09-22 版，含 SD：6 条 `/api/sd/*`，**仍缺 `/stat`、不认 `overwrite`**）· latest=`1.0.2`。
   校验固件别信 `esp_app_desc.date`（重链不刷新，实测文件 16:42 / 字段写 00:49），也别信 `fw_version`（两次构建同名 `2413698-dirty`）——**用 sha256 比对 + 在 bin 里 grep 关键字符串**。
2. 控制台 `/ctrl` 选版本 → 服务端 `ota_start()` 下发 `url` + `sha256`（**默认不发 `version`**，原因见下）。
3. 板端 `ota_service.c` 手工 `esp_http_client` 分块下载 + `esp_ota_write`（**未使用** `esp_https_ota`），逐帧回 `ota_status`；服务端 `ota_absorb_report()` 吸收（**同时兼容扁平 `ota_*` 字段与嵌套 `ota` 对象**两套写法）。
4. `READY_TO_REBOOT` → 板端重启 → 重连后 hello 触发 `ota_on_hello()` 收尾（**重启导致的 WS 断连不算中断**）。
5. 回滚确认链路：`main.c:115` → `ota_service.c:561` 完整。

- ⚠ `ota_service.c:368` 用 `strncmp` 比对 `args.version` 与镜像内嵌 `esp_app_desc.version`，**不符即 `FAILED(INVALID_VERSION)`** → 所以服务端**默认不下发 version**。
- ⚠ 固件 URL 必须用 `_lan_ip_for(device_id)` 按**设备所在网段**挑本机 IP（本机常有多张网卡，`local_ips()[0]` 往往是校园网口，直接用会 404）。
- `read_app_desc()` 解析镜像内嵌 `esp_app_desc`（**offset `0x20`，magic `0xABCD5432`**）。目录名（如 `1.0.1`）与镜像内嵌的 git describe 串**不是一回事，不要去"统一"**；固件一重编串就变。
- **当前固件库落后一代**：库里仍是 09-19 版本（`sha256 022fb241…`），板端 09-20 新构建（`sha256 02e96278…` / `7462683`）**尚未拷入** —— 见 §10.2。

## 9. 板端（**只读，禁止改**）

- 仓库：`FJ.git`，HEAD `2413698`；分区表 `partitions_ota.csv`：`nvs`@0x9000 24K / `otadata`@0xF000 8K / `phy_init`@0x11000 4K / **`ota_0` 4MB @0x20000** / **`ota_1` 4MB @0x420000** / **`storage` 7.875MB @0x820000**（spiffs，**当前完全空闲，全仓无 `esp_vfs_spiffs` 调用**）。
- app 实际只占 **1.34MB / 4MB**（每槽余 2.66MB）→ **不需要扩 app 分区**；字库等小资源直接编进 app（自动享受双槽 + 回滚）。
- **只有 app 分区可安全 OTA**；bootloader / 分区表 / NVS 等非 app 分区技术上能写但**无备份、断电即坏** → **一律按"只能串口"处理**。
- ESP-SR（语音唤醒）需要的模型分区**标签必须叫 `model`**（`movemodel.py` 打包 `srmodels.bin`），与 UI 资源应分属两个分区；模型更新走**两段式**（先 OTA app → 新 app 启动时刷 model）。
- 为什么不让 AI 改板端：**板端一改就要重烧**（用户需手动接线、走串口，成本高）。所以既定策略是"**能在服务端归一/兼容的，绝不动板子**"。自 09-19 起板端代码 **AI 一行没改过**，现有提交全部由用户本人完成。
- 引脚（需要加外设时参考）：servo UART **17/18** · mic I2S **14/15/16** · spk I2S **7/8/21** · TFT(SPI2) SCLK**12**/MOSI**11**/CS**10**/DC**6**/RST**5**/BL**4** · PTT **9** · LED **48**。**空闲 13、19、20**（19/20 是 USB D±）。
  ⚠ **SD 卡不再按"复用 TFT 的 SPI2"规划**（旧笔记已过时）：板端已用**独立 SPI3** 占 **SCLK1/MOSI2/MISO41/CS42**，`app_config.h:40-44`，20MHz，开机即挂载（`main.c:104`）。41/42 兼外部 JTAG 脚，USB 烧录无影响。

## 10. 已知 bug / 待办（接手后优先看这里）

1. **`ota_absorb_report()` 误判（09-21 发现，未修，纯服务端约 6 行）**
   板端**每帧周期 `state` 都带扁平 `ota_state:"IDLE"`**，而 `servev1.py:2324` 的放行条件（`board_state` / `ota_active` / 嵌套 `ota` 三者全空）永不成立 → **普通 state 也会建 OTA 记录**，日志出现 `板端相位 IDLE -> preparing 0%` 紧跟 `IDLE -> succeeded 0%`：板子没做 OTA，控制台却显示"升级成功"。
   修法：本地无活跃会话（无记录或已到终态）+ `board_state == "IDLE"` + 非 active → 在进 `DEV_LOCK` 之前直接 `return False`。
2. **固件库落后一代**：`firmware/esp32s3/1.0.1/app.bin` 仍是 09-19 23:12 版；板端 09-20 19:29 的新构建未拷入（见 §8.1 流程）。
3. **`dev-v2` 废弃分支**待清理（见 §3）。
4. **`README.md` 严重滞后**（还在写 faster-whisper 阶段、GUI `:8080`、`--model models/faster-whisper-base`），需要重写或加"现状说明"；`XIAOZHI.md`（09-14）同样需要校对。
5. `dog_actions.json` 里 group 1..12 仍是**占位编号**，需按实际控制板改成真实动作组。
6. 板端网页由 `web_page.src.html` 转义生成 `web_page.h`（改网页要改源文件再生成，别直接改 `.h`）。
7. 板端改动：**SD 卡挂载已完成**（`main/sd_card.c` 挂 `/sdcard`，独立 SPI3）——**但改动全未提交**；剩下的是**接口层全空白**，按 `../SD卡控制台-板端接口清单.md` 补 8 条 HTTP 路由 + 3 个 state 字段即可（见 §7.1，白名单/分发表不用动）。其余待做：分区表重划（给 `model` 分区腾位置）、语音唤醒（WakeNet9 / MultiNet）接入。
   唤醒备忘：自定义唤醒词走 **MultiNet 拼音法**（如 `xiao gou xiao gou`，**免训练**）或乐鑫 TTS 代训；⚠ **WakeNet 的训练工具从未开源**，网上"自己训练 WakeNet"的教程是虚构的。

## 11. 改完必须验证

```bash
# 三个自测脚本都不需要板子在位（_test_*.py 已 gitignore）
python _test_ota.py          # 53 项：OTA 状态机 / 固件索引 / sha256
python _test_adapt.py        # 42 项：两端协议适配 / 状态归一 / 音频协商
python _test_sim_board.py    # 模拟板子对真实服务握手（⚠ 会往 device_endpoints.json 写 sim-board-01，跑完手动删）

# 另有交互式/回归脚本
python test_client.py --wav <16k 单声道说话录音.wav>   # 期望：hello 应答 OK → N 帧已发送 → 收到 stt → PASS
python verify_board_adapt.py ; python verify_commands.py ; python verify_twin.py
```

- 真机联调判据：日志依次出现 `hello: version=3 audio=16000Hz/60ms` → `已绑定音频 WS` → `device.status` 回固件版本 → 指令 accepted → TTS 播放 `SPEAKING → IDLE`。
- `test_client.py` 用 `websockets.connect(url, proxy=None)` 强制直连 —— 本机若开着代理（`HTTP_PROXY`），WS 会被代理拦成 502。

## 12. 数据文件与术语

| 文件 | 说明 |
|---|---|
| `llm_config.json` | 大模型配置，**含真实密钥 → 已 gitignore，切勿提交** |
| `devices.json` / `device_endpoints.json` | 设备登记与端点（host/port/token），运行时写入 |
| `dog_actions.json` | 动作组定义（group 编号仍是占位值，见 §10.5） |
| `twin_store.json` / `twin_store.example.json` | 数字孪生运行时状态 / 模板 |
| `firmware/firmware.json` | 固件版本索引（**入库**）；同目录 `*.bin` 不入库 |
| `opus.dll` | Windows 下 `opuslib_next` 的解码依赖，仓库内自带（Linux 用系统 libopus） |

- 术语：`sequence.run` = **服务端**位置序列下发；`action.run` = **控制板**预存动作组执行 —— **两套数据源，不可合并**。
- 用户口令习惯：说"**把服务挂起来**" = **启动并保持后台运行**，不是 stop/suspend。
