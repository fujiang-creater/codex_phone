# 板端（esp32s3 固件）OTA 需求清单

**范围**：把服务端已就位的 OTA 骨架接上，让 `ota.start` / `ota.status` 真正能跑通一次真机升级。
**视角**：本文只写**板端要做什么**；服务端需要配合的部分单列在第五节。

**现状一句话（写于板端实现之前，已被下文更新节取代，保留作对照）**：服务端（会话状态机、
固件表、下载路由、四个 HTTP 端点）已就绪；当时板端为空壳 —— `ota.start` 返回
`501 NOT_IMPLEMENTED`（`command_dispatcher.c:471-476`），`ota.status` 同样直接 501（`:477-478`）。

> **09-19 23:04 起板端已实现、服务端已全部对齐，两端链路可用** —— 见下方"实现状态更新"节。
> 本文其余章节（第一~七节）是当时的**需求方案**，作为设计依据留存。

**结论先行**：板端 OTA 不复杂，因为 IDF **有现成组件**（`esp_https_ota`），
不用手写"下载 + 写 flash"循环。板端真正的工作量在**对接契约**（提供状态、守安全边界），
不在于 OTA 算法本身。

---

# ⚠️ 实现状态更新（09-19 23:04）：板端已完成，上文部分结论已过时

板端**已实现 OTA**（`main/ota_service.c` 566 行 + `ota_service.h`，22:55 写就），
上文"板端零实现"的前提已不成立。以下是逐项对账。

## 板端已落实的（按实际代码核对）

| 项 | 位置 |
|---|---|
| `ota.start` 真实现（解析 `url` / `version` / `sha256`，同步回 `accepted`） | `command_dispatcher.c:540-565` |
| `ota.status` 真实现 | `command_dispatcher.c:305`、`:566` |
| 下载 + 写分区（手工 `esp_http_client` + `esp_ota_write`，**未用** `esp_https_ota`） | `ota_service.c:198-360` |
| HTTP 状态码 / Content-Length / 总长度一致性校验 | `ota_service.c:232-233`、`:324-330` |
| SHA-256 校验（PSA，可选传入） | `ota_service.c:269-340` |
| 启动确认（回滚确认） | `main.c:115` → `ota_service_confirm_pending_boot()` |
| 回滚保护开关 | `sdkconfig:698` `CONFIG_BOOTLOADER_APP_ROLLBACK_ENABLE=y` |
| 升级前停服务 | `command_dispatcher.c:553` `stop_non_ota_services()` |
| OTA 期间屏蔽音频（丢二进制帧、拒绝开采集） | `audio_ws.c:542`、`:785`；`audio_capture.c:132`、`:326` |
| **当前固件处于 PENDING_VERIFY 时拒绝再 OTA**（很有价值的一条） | `ota_service.c:490-496` |
| 构建依赖 | `CMakeLists.txt:21,28,29` 加了 `ota_service.c` / `esp_http_client` / `app_update` |

> 关于 API 选型：板端走的是手工 `esp_http_client` + `esp_ota_write`，不是我之前建议的 `esp_https_ota`。
> 在"要自定义进度上报颗粒度"这个需求下，手工路线更可控，选它没问题；
> 也正因如此 `CONFIG_ESP_HTTPS_OTA_ALLOW_HTTP` 不需要开（那是 `esp_https_ota` 组件自己的限制）。
> 上文第三节里"三个 sdkconfig 开关"因此只剩回滚保护那一项是必需的。

## 板端实际上报格式（与上文第一节第 3 项不同，**以代码为准**）

**周期 state**（`audio_ws.c:213-240`）用**扁平字段**：

```json
{"type":"state","device_id":"...","state":"OTA",
 "ota_state":"DOWNLOADING","ota_active":true,
 "ota_progress_percent":42,"ota_written_bytes":550000,"ota_total_bytes":1300000}
```

**状态变化事件**（`audio_ws.c:266-288`）用**嵌套对象**（与你给的协议文档一致）：

```json
{"type":"state","state":"OTA",
 "ota":{"state":"READY_TO_REBOOT","progress_percent":100,
        "written_bytes":1300000,"total_bytes":1300000,"error":0}}
```

`state` 字段的取值是 **`"OTA"`**（`audio_ws.c:178-180`），不是 `UPGRADING`。

## 我上文判断错的两条（更正）

1. **"OTA 期间 WS 已停，进度必须走 HTTP" —— 不成立。**
   板端实现是 OTA 全程**保持 WS 连接**（只丢弃音频二进制帧），只有最后重启那一下才断。
   所以进度走 WS 完全可行 → **不需要 HTTP 轮询，也不需要服务端新增拉取能力**。
   上文第一节第 3 项"上报通道必须换"、第五节第 3 条，**作废**。
2. **"版本号必须先统一" —— 不阻塞升级。**
   板端 `expected_version` **不做任何比对**：`ota_service.c` 里 grep 不到
   `esp_app_get_description` / `strcmp` / 镜像头解析，它只是转存 + 在上报里带出来。
   所以两端版本号不一致**不会导致升级失败**，只影响服务端"该不该升"的判断。
   上文第五节第 4 条**降级为建议**。

## ✅ 服务端侧已全部补齐（09-19 23:2x 完成，本节由"待解"改为存档）

| # | 原问题 | 处置 |
|---|---|---|
| 1 | 固件表指向假固件（1015 字节 DUMMY） | 已用真镜像覆盖 `firmware/esp32s3/1.0.1/app.bin` = **1,339,120 字节**，`sha256=022fb241…9751`。该 `*.bin` 已加进 `.gitignore` 并从索引移除（1.3MB 二进制且每次 build 都变，不进仓库） |
| 2 | 固件下载路由要鉴权（板端裸 GET 401） | `/firmware/esp32s3/{ver}/app.bin` **已放开鉴权**（板端 esp_http_client 不带 token、只吃 http）；准入仍由上游 `ota.start` 把关 |
| 3 | 不认识 `state:"OTA"` 与进度字段 | `DEV_STATES` 已加 `"OTA"`；`ota_absorb_report()` 同时解析**扁平 `ota_*` 字段**与**嵌套 `ota` 对象**，相位/百分比/字节数/错误信息全部入库；控制台 OTA 列直接显示"下载中 42%" |
| 4 | 控制台**选不了固件版本**（只有"OTA 升级"一个按钮，`POST {}` 永远走 latest） | `ctrl.html` 加版本下拉 + `刷新固件列表` + `强制版本校验` 勾选；服务端列表接口补 `build_version`/`url` 并去掉本机 `path`；`read_app_desc()` 解析镜像内嵌 `esp_app_desc`（offset `0x20`） |

**顺带修掉的两个新坑**（都是实测发现，不修会直接影响升级成功率）：

- **`ota.start` 不再下发 `version`**：板端 `ota_service.c:368` 会拿 `args.version` 与镜像内
  `esp_app_desc.version` 做 `strncmp`，不一致直接 `FAILED(INVALID_VERSION)`。服务端目录名（`1.0.1`）
  与固件内嵌版本（板端 git describe 串）压根不是一回事 → 默认只下发 `url` + `sha256`；
  确需强制版本校验时调 `ota_start(..., send_version=True)` —— 此时服务端会**自动改发该镜像内嵌
  的 `esp_app_desc.version`**（控制台勾"强制版本校验"即走这条路），读不出内嵌版本则直接拒绝，
  免得把目录名当校验值发下去白跑一趟。当前真镜像内嵌版本 = `52dff9c-dirty`（idf v6.0.2），
  **固件一重编这串就变**，所以任何地方都不要硬编码它、也不要试图和目录名"统一"。
- **固件 URL 按设备网段选本机 IP**：本机有 4 张网卡（`10.200.137.69` 校园网 / `192.168.137.1` ICS 热点 /
  `172.27.112.1` WSL / `172.20.176.1`），`local_ips()[0]` 是校园网口，板子根本够不着。
  新增 `_lan_ip_for(device_id)`：优先与设备上报 IP 同 /24 的本机地址，其次任何 `192.168.*`。

- **断连语义**：板端 `READY_TO_REBOOT` 后自己重启，这类 WS 断开**不再**被标 `interrupted`；
  等它重启回来重新 `hello` 时才判 `succeeded`（`ota_on_hello()`）。

自测：`python _test_ota.py`（51 项断言全通过，不需要板子在位）。

**控制台怎么用**：`/ctrl` 页 OTA 区下拉选版本 → 建议先**不勾**"强制版本校验"（走 sha256，
板端本来就会做完整性校验）→ 点"OTA 升级"，确认框会列出设备/版本/内嵌版本/大小/校验方式。
新增可用版本 = 建目录 `firmware/esp32s3/<版本>/app.bin` 后点「刷新固件列表」
（默认选中项由 `firmware/firmware.json` 的 `latest` 决定，不写则取版本号最大者）。

---

## 剩余待解（这三条才是真的会卡住）

| # | 问题 | 说明 |
|---|---|---|
| 1 | **服务端固件表指向假固件** | `firmware/esp32s3/1.0.1/app.bin` 只有 **1015 字节**（`DUMMY-FW-1.0.1`）。`load_firmware_index()` 据此算出的 `size`/`sha256` 全是假的，下发必被板端 SHA-256 校验拒绝。真固件在板端 `build/esp32s3n16r8_idf.bin`，**1,339,120 字节** |
| 2 | **固件下载路由要鉴权** | `servev1.py:2633` 的 `_auth_ok(None)` → 板端裸 GET 会 401。板端只支持 `http://` 且不带 token，需给该路由开白名单（或另起一个无鉴权文件服务） |
| 3 | **服务端不认识 `state:"OTA"` 与进度字段** | `DEV_STATES`（`servev1.py:973`）**没有 `"OTA"`**，`dev_set_state` 遇未知状态直接 `return False`（`:1107-1108`）→ OTA 期间周期上报被静默丢弃、`last_seen` 不刷新；`ota_state` / `ota_progress_percent` / 嵌套 `ota` 对象**服务端一处都没解析** → 控制台与孪生页看不到进度 |

**第 3 条的好消息**：门禁并不依赖这些字段。`dev_gate`（`servev1.py:1328`）看的是设备 state `== "UPGRADING"`，
而 `UPGRADING` 是服务端自己在 `ota_start()`（`:2094`）里设的 —— 所以**升级期间门禁照常生效**，
且 `UPGRADING` 不参与 `STATE_TTL_S` 过期判定（`:1170`），不会被误判离线。
重启后板端发 `state:"IDLE"`，门禁自动解除。
换句话说第 3 条是**体验问题**（看不到进度），不是功能阻塞 —— 除非要控制台显示进度条。

---

## 一、板端要向服务端"提供"的四类信息

这四项是服务端判断"该不该升、升到哪了、升成没成"的依据。

### 1. 能力声明（放在 `hello` 里）

| 字段 | 值 | 为什么 |
|---|---|---|
| `supports_ota` | `true` | 现在服务端只能靠收到 501 才知道板子不支持 OTA，纯试错 |
| `ota_partition_size` | OTA 分区可用字节数（可选） | 让服务端在下载前就能否掉"固件放不下" |

### 2. 运行槽位与版本（`state` / `device.status` 里）

| 字段 | 来源 | 现状 |
|---|---|---|
| `running_slot` | `esp_ota_get_running_partition()` → `"ota_0"` / `"ota_1"` | **缺失**。服务端 `METRIC_FIELDS`（`servev1.py:983-984`）**已经在等这个字段**，板端补一行就能接上，服务端零改动 |
| `fw_version` | `esp_app_get_description()->version` | **已有字段但值是错的**：板端报 `0e8a6f1-dirty`（git describe），服务端固件表是 `1.0.1`。OTA 靠版本号判断"要不要升/升成没成"，**两者必须先统一** |

`running_slot` 的额外价值：OTA 成功重启后，槽位会从 `ota_0` 翻到 `ota_1` ——
这是服务端确认"升级真的生效了"最直接的证据（比版本号更难伪造）。

### 3. OTA 进度与终态（升级期间，**走 HTTP 不走 WS**）

| 字段 | 取值 | 说明 |
|---|---|---|
| `ota_phase` | `idle` / `preparing` / `downloading` / `verifying` / `succeeded` / `failed` / `rollback` | 直接对应 `esp_https_ota` 的事件枚举，不用自己造状态机 |
| `ota_written` | 已写字节数 | `esp_https_ota_get_image_len_read()` |
| `ota_total` | 固件总字节数 | `esp_https_ota_get_image_size()` |
| `ota_error` | 失败原因字符串 | 失败时必填，否则服务端只能干等超时 |

**上报通道必须换（关键设计点）**：OTA 期间 WS（8765）已经停掉，
所以进度**不能**走 WS。这一点在你自己的注释里已经写明了 ——
`servev1.py:971`：

> 状态由板子周期上报（平时走 WS 文本帧，**OTA 阶段走 HTTP**，因为那时 WS 已停）

落地方式：让板端现有的 `GET /api/v1/status`（`control_http.c:754`）在 OTA 期间返回上面这几个字段，
服务端改用 HTTP 轮询（服务端这半边也要改，见第五节）。

### 4. 前置条件可查询

| 项 | 现状 |
|---|---|
| 电量 `GET /api/battery` | **已有**（`control_http.c:748`）。OTA 中途断电是唯一能真正刷砖的操作，开工前应先看电压 |

---

## 二、板端要"实现"的两个命令

服务端下发的参数形状我已经核实（`servev1.py:2083-2105`）：

### `ota.start`

```jsonc
// WS 命令（mode=sync, policy=interrupt）携带的 args
{
  "version": "1.0.1",
  "url":     "http://<服务端LAN IP>:8000/firmware/esp32s3/1.0.1/app.bin",
  "size":    1310576,
  "sha256":  "<hex>"
}
```

**语义要求**：

1. **同步回执要快** —— `mode=sync`，板端收下并校验完入参后立刻回 `ok`（别等下载完，那是 10 分钟）
2. **校验三件事，任何一项不过立刻回失败 + 原因**：
   - 版本比当前新（用 `esp_https_ota_get_img_desc()` 在**下载前**读固件头的版本号比对，不满意就 `abort`，一个字节都不写）
   - `size ≤ OTA 分区可用空间`（分区 4MB，固件 1.25MB，够）
   - 电量够
3. 通过则**起后台任务**执行升级，立即返回 `accepted`

### `ota.status`

```jsonc
// args 为空对象 {}（mode=sync, policy=queue）
// 返回：上面第一节第 3 项的四个字段
```

**注意这个命令本身有悖论**：服务端 `ota_status()`（`servev1.py:2108-2110`）是**通过 WS** 发的，
而 OTA 期间 WS 已停 → 这个查询在升级过程中天然不可用。
所以它只在**非升级期间**有意义（查上次结果），升级中的进度必须走 HTTP。服务端这半边也要改。

---

## 三、板端要"改"的文件与配置

### 代码

| 文件 | 改动 |
|---|---|
| **新建** `ota_update.c/h` | 用 `esp_https_ota_begin` / `perform` / `finish` + 事件回调；对外暴露 `ota_update_start(url, size, sha256)`、`ota_update_get_progress()`、`ota_update_is_running()`。**不要手写下载循环** |
| `command_dispatcher.c` | `ota.start`（`:471-476`）换成真实现；`ota.status`（`:477-478`）换成真实现；`add_common_status()`（`:253-274`）补 `running_slot` 与 OTA 字段 |
| `main.c` | 启动健康自检通过后调 `esp_ota_mark_app_valid_cancel_rollback()` |
| `main/CMakeLists.txt` | `REQUIRES` 加 `esp_https_ota`（自带 `app_update`）和 `esp_http_client` —— **当前两者都没有** |
| `control_http.c` | `GET /api/v1/status`（`:754`）的响应体里带上 OTA 进度字段 |

### 配置（`sdkconfig`，三个开关现在全是默认关）

```
CONFIG_ESP_HTTPS_OTA_ALLOW_HTTP=y               # 服务端是 http 明文，必须开
CONFIG_BOOTLOADER_APP_ROLLBACK_ENABLE=y         # 回滚保护，强烈建议
CONFIG_ESP_HTTPS_OTA_ENABLE_PARTIAL_DOWNLOAD=y  # 可选，断点续传
```

### 顺带该开的一项（与 OTA 无关，但和之前的 TTS 断续有关）

```c
esp_wifi_set_ps(WIFI_PS_NONE);   // OTA 期间必开，提吞吐
```

理由：官方 `simple_ota_example.c:206-211` 特意写了这段，注释是"确保关掉 WiFi 省电，
以获得最佳吞吐和整体 OTA 时序"。而你板子 sdkconfig 里**没有任何显式 PS 配置**，
串口日志里那行 `wifi:pm start, type: 1` 说明用的是**运行时默认省电模式**
（type 1 = `WIFI_PS_MIN_MODEM`）。省电模式会周期性休眠、只在 DTIM 窗口收包 ——
这与你板端只有 120ms preroll 缓冲叠加，很可能就是 **TTS 断续的第二个原因**
（第一个是已定位的 I2S tick 单位 bug，两者独立，要分开验证）。

---

## 四、板端必须守的安全边界

1. **先回执，再重启**。HTTP 200 必须**先发出去**再重启，否则服务端永远收不到回执。
   惯常做法：延迟 ~500ms 再 `esp_restart()`。板端已有这个模式可参考 ——
   `reboot_task()`（`command_dispatcher.c:246-251`，250ms 延迟重启）。
2. **回滚保护**。新固件起来后，**等确认自己真的健康**（WiFi 连上 → WS 连上 → 服务端收到 hello）
   才调 `esp_ota_mark_app_valid_cancel_rollback()`。不调的话，一旦新固件是坏的，
   板子**没有自救退路**。硬件条件已具备：双分区 `ota_0`/`ota_1` 各 4MB（`partitions_ota.csv`，
   `CONFIG_PARTITION_TABLE_CUSTOM=y` 已生效）。
3. **失败要立刻回原因**，别让服务端干等到 `OTA_TTL_S=600`（`servev1.py:976`，10 分钟）才判超时。
4. **升级前停服务**：已有 `stop_non_ota_services()`（`command_dispatcher.c:85-91`，
   停采集 / 播放 / 序列 / 动作），`ota.start` 里已经在调 —— 保留。
5. **边下边写**，不要整包读进内存（固件 1.25MB）。
6. **OTA 期间置设备状态为 `UPGRADING`**，服务端会据此做全量门禁。

---

## 五、依赖：服务端要配合的四件事（会卡住板端）

这四条不在板端，但不解决板端就跑不通。
**（订正：第 3、4 条已按上文集头部"实现状态更新"作废/降级，实际只剩第 1、2 条是硬阻塞。）**

| # | 问题 | 位置 |
|---|---|---|
| 1 | **固件是假的** —— `firmware/esp32s3/1.0.1/app.bin` 只有 **1015 字节**，内容是 `DUMMY-FW-1.0.1-...`。据此算出的 `size`/`sha256` 全是假的 | `firmware/` |
| 2 | **下载路由要鉴权** —— `_auth_ok(None)` 会让板端裸 GET 直接 **401**。要么给板端配 token，要么给该路由开白名单 | `servev1.py:2633` |
| 3 | **`ota_status()` 走 WS** —— OTA 期间 WS 已停，升级中的进度查询天然不可用。服务端需新增**从板子拉取** `/api/v1/status` 的能力（目前服务端作为 HTTP 客户端只 POST 到 `/api/v1/command`，没有 GET 拉取） | `servev1.py:2108-2110`、`:2183-2202` |
| 4 | **版本号体系不统一** —— 板端 `0e8a6f1-dirty`（git describe）vs 服务端固件表 `1.0.1`。需选定一套 | 两端 |

另外：服务端 `POST /api/v1/devices/{did}/ota/ready` 和 `/ota/finish`（`servev1.py:2982-2990`）
是给板端主动上报用的，但**整段有鉴权**（`:2936`），板端要调必须带 token。

---

## 六、现状盘点（速查）

### ✅ 板端已有，可直接复用

- `stop_non_ota_services()` —— 升级前停服务（`command_dispatcher.c:85-91`）
- `reboot_task()` —— 延迟重启模式（`:246-251`）
- 双 OTA 分区 `ota_0` / `ota_1` 各 4MB，`CONFIG_PARTITION_TABLE_CUSTOM=y` 已生效
- `GET /api/battery` —— 电量前置检查（`control_http.c:748`）
- `GET /api/v1/status` —— 进度上报的载体（`:754`）
- `fw_version` 字段（值需修正）
- 命令分发框架（信封校验 / request_id 去重 / 回执格式）完整可用

### ❌ 板端缺失，需要新增

- 整个 OTA 实现（`esp_ota_*` / `esp_https_ota` 调用）
- `ota.start` / `ota.status` 真实现（现在是 501 占位）
- `running_slot` 上报
- OTA 进度字段（`ota_phase` / `ota_written` / `ota_total` / `ota_error`）
- 构建依赖 `esp_https_ota`、`esp_http_client`
- 三个 sdkconfig 开关
- 启动时的回滚确认调用
- HTTP 客户端能力（当前无 `esp_http_client`，也就无法主动 POST 到服务端的 `/ota/finish`）

---

## 七、建议的实施顺序

**（板端已于 09-19 22:55 实现完毕，本节仅作脉络留档；当前待办请见文首"剩余待解"。）**

先打通链路，再加精度，别一上来就调状态机：

1. **统一版本号**（两端约定，`PROJECT_VER` 写死 `1.0.1` 之类）
2. **补 `running_slot` 上报** —— 服务端零改动就能接，先拿这个验证"上报通道通不通"
3. **sdkconfig 三开关 + CMakeLists 加依赖**
4. **写 `ota_update.c`，先用 `esp_https_ota` 一步到位版**（`esp_https_ota(&config)` 一次调用完成），
   把"能不能升级成功"这条链路跑通
5. **跑通后再改成 `begin/perform/finish` 流式版**，加事件回调与进度上报
6. 最后补回滚保护与 HTTP 进度轮询

第 4 步是关键分界：**第一次烧录就能看到"升级成功/失败"**，而不是调试一堆半成品状态机。
