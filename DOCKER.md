# 服务端容器化（Docker）· 使用说明

**这个镜像装的是 PC 端 Python 服务中心**（`servev1.py`：:8000 控制台 + :8765 与板子通信）。
它的价值不是"单次安装更快"，而是：**换机器不用重装 120 个包**、**不再踩平台相关的编译轮子 / Python 版本 / 系统库**、
以及**断网也能搬**（`docker save`）。

> 边界：**ESP-IDF 编译固件不在这里面**。OTA 用的 `app.bin` 仍由板端工程 `idf.py build` 产出，
> 再手工拷进 `firmware/esp32s3/<版本>/`（这一步不变，见 `PROMPT.md` §8）。

---

## 1. 前置

```bash
docker --version            # 需要 Docker Engine + compose v2
                            # Ubuntu: apt 装 docker.io（或 docker-ce）+ docker-compose-plugin
```

## 2. 快速开始

```bash
cd server
docker compose up -d --build      # 第一次要 build，10~30 分钟（下载 120 个包）
docker compose logs -f            # 看日志
```

- **就绪判据**：日志出现 `[server] WebSocket :8765` 那一行才算真的起来了（启动要 1.5~3 分钟，慢在 FunASR 加载模型）。
  也可以看 `docker ps` 的 STATE 是不是 `healthy`（健康检查的 start-period 已给到 4 分钟）。
- 页面：浏览器开 `http://127.0.0.1:8000/`；板子连 `ws://<本机局域网IP>:8765?role=xiaozhi`。
- 改动 `servev1.py` 后**不用重建镜像**（代码是挂载进去的），只需 `docker compose restart`。

## 3. 模型从哪来（901MB）

首次启动会自动从 ModelScope 下载到卷里。想省这 900MB，就把旧机的缓存灌进卷：

```bash
docker compose up -d --build                      # 先把镜像建出来
docker compose stop
docker run --rm --entrypoint sh \
  -v server_modelscope-cache:/dest \
  -v "$HOME/.cache/modelscope:/src:ro" \
  dog-server:latest -c 'mkdir -p /dest && cp -a /src/. /dest/'
docker compose up -d
```

（卷名 = 目录名 + `_modelscope-cache`，`docker volume ls` 可确认。）

## 4. 为什么必须用 host 网络

`_lan_ip_for()`（`servev1.py:2601`）要靠**宿主网卡**挑一个"与板子同网段"的 IP 下发给板子做 OTA 固件 URL。
桥接模式下容器只有 `172.17.0.x`，三级判据（同 /24 → `192.168.*` → `local_ips()[0]` 兜底）全部落空
→ 固件 URL 变成容器内网地址 → **板子下载 404**；而 `:8765` 因为端口映射照常通，
于是呈现"**能连上、能对话，只有 OTA 失败**"的半通状态。→ 所以 `docker-compose.yml` 用的是 `network_mode: host`。

代价：容器与宿主共享网络栈，不再有网络隔离。对这台专用机器可接受；若要隔离，就得改桥接
并给服务端加一个"对外 IP"覆盖参数（见 `PROMPT.md`）。

## 5. 哪些东西在镜像里、哪些在宿主机

| 内容 | 位置 | 说明 |
|---|---|---|
| 120 个 Python 包 + 系统库 | **镜像** `/opt/venv` | 版本由 `requirements.lock.txt` 锁死 |
| 代码 `servev1.py` 等 | 宿主机 `./` → `/app` | 挂载进去，改完 restart 即生效 |
| `llm_config.json`（真实密钥） | 宿主机，**不进镜像** | `.dockerignore` 已排除；镜像分层能挖出旧文件，事后删也白搭 |
| 固件 `firmware/**/app.bin` | 宿主机，**不进镜像** | OTA 每次手工更新，挂载进去即可 |
| 模型缓存 901MB | 命名卷 `modelscope-cache` | 见 §3 |
| 运行时状态 `device_endpoints.json` / `twin_store.json` | 宿主机 `./` | 随代码目录一起持久化 |

## 6. 搬到别的机器

**有网** —— 推镜像（比在每台机器 build 快得多）：

```bash
docker tag dog-server:latest <你的仓库>/dog-server:latest
docker push <你的仓库>/dog-server:latest
# 新机器：
git clone <server 仓库> && cd server
docker compose up -d            # 只 pull，不 build（模型见 §3）
```

**断网** —— 导出 tar 拷过去：

```bash
# A 机
docker save dog-server:latest | gzip > dog-server.tgz     # 约 3~4GB
# B 机（把 tar 和代码目录一起拷过去）
gunzip -c dog-server.tgz | docker load
docker compose up -d
```

## 7. 常用命令

```bash
docker compose logs -f --tail 100     # 追日志
docker compose restart                # 改完代码重启
docker compose down                   # 停掉（保留模型卷）
docker compose down -v                # 连模型卷一起删（下次要重下 901MB，慎用）
docker compose exec serve sh          # 进容器排查
```

## 8. 排错

- **build 卡在 torch**：`ERROR: No matching distribution found for torch==2.14.0+cpu`
  说明你用到的 PyTorch CPU 索引上没有这个版本的 Linux 轮子。两条路：
  1. 先把版本降回索引里确实有的（**2026-09-22 实测：cp311/linux_x86_64 最高 `2.6.0+cpu`，
     cp311/win_amd64 最高 `2.14.0+cpu`** —— 也就是说 `2.14.0+cpu` 这个 pin 是 Windows 专有，
     Ubuntu 上必须降版本）。查有哪些版本：
     `curl -s https://download.pytorch.org/whl/cpu/torch/ | grep -o 'torch-[0-9][^"]*cp311[^"]*linux_x86_64.whl' | sort -u`
     ⚠ **按数值排序看，别用字符串排序** —— 字符串序会把 `2.14.0` 排到 `2.9.1` 前面，容易看漏新版本。
  2. **不要直接把 `+cpu` 去掉图省事** —— PyPI 上 Linux 的默认 `torch` 是 **CUDA 构建**，会顺带拉一堆 `nvidia-*`，
     镜像直接涨 2~3GB。真要这么改，把 `requirements.lock.txt` 里 `torch` / `torchaudio` 两行的 `+cpu` 一起去掉，保持一致。
- **容器起来了但板子连不上**：先确认 host 网络生效 —— `docker compose exec serve ip addr` 里应该看到宿主的
  `192.168.x.x`（看到 `172.17.x.x` 说明还在桥接）；再看 `sudo ufw allow 8765/tcp`。
- **`healthy` 一直不出现**：正常，FunASR 加载要 1.5~3 分钟；看日志有没有 `[asr] FunASR ready`。
- **OTA 一开就 404**：回到 §4，基本是网络模式的问题。
- **改了 `.py` 不生效**：代码是挂载的，确认 `docker compose restart` 过；改的是 `Dockerfile`/依赖 才需要 `--build`。

## 9. 已知差异（和原生部署比）

- `--pc-input`（把识别结果打进 PC 焦点窗口）、剪贴板相关功能**是 Windows 专有**（代码在 `if os.name == "nt"` 里），
  Linux 容器里没有，不会崩、只是功能不可用。
- 不需要 GPU（torch 是 CPU 版），也不碰串口/声卡 → 无需 `--device` 或 `/dev/snd` 直通。
- 容器内是 root + host 网络，属于"图省事"的取舍；实验机可以，别拿它跑不受信任的代码。
