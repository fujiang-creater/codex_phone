# ============================================================================
# 机器狗服务端 · Dockerfile（Linux 目标）
# ----------------------------------------------------------------------------
# 设计要点：
#   · 分层：依赖装进 /opt/venv（构建阶段），运行时只拷 venv → 镜像里不留编译器
#   · 依赖用 requirements.lock.txt 的 120 个精确版本；Python 锁 3.11（FunASR 依赖链要求）
#   · torch / torchaudio 是 +cpu 版，必须带 --extra-index-url 才能取到
#   · 运行时系统库只有 4 个：libopus0（opuslib_next 解码）、libsndfile1（soundfile）、
#     libgomp1（onnxruntime / scipy / sklearn 的 OpenMP 依赖，slim 基础镜像里没有）、
#     tzdata（日志时间）—— av(PyAV) 自带 FFmpeg，不需要另装
#   · 本项目**没有本地音频设备依赖**（不用 sounddevice / pyaudio），所以不需要 /dev/snd 直通
#   · 密钥 / 模型缓存 / 固件 bin 都**不进镜像**（见 .dockerignore 与 DOCKER.md）
#
# 用法：docker compose up -d --build        详细步骤见 DOCKER.md
# ============================================================================


# ---------- 阶段 1：装依赖 ----------
FROM python:3.11-slim-bookworm AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# build-essential：crcmod 等少数包只有 sdist，需要编译器
# libopus0 / libsndfile1 / libgomp1：让构建期那条 import 自检能过（也方便早发现问题）
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential libopus0 libsndfile1 libgomp1 \
 && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.lock.txt /tmp/requirements.lock.txt
RUN pip install -U pip \
 && pip install --extra-index-url https://download.pytorch.org/whl/cpu -r /tmp/requirements.lock.txt \
 && python -c "import torch, funasr, onnxruntime, websockets, opuslib_next, soundfile, av, numpy, scipy, sklearn, librosa, jieba, modelscope; print('deps ok, torch', torch.__version__)"


# ---------- 阶段 2：运行时 ----------
FROM python:3.11-slim-bookworm AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Shanghai \
    PATH="/opt/venv/bin:$PATH"

RUN apt-get update \
 && apt-get install -y --no-install-recommends libopus0 libsndfile1 libgomp1 tzdata \
 && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY . /app/

# 启动要 1.5~3 分钟（FunASR 加载模型），start-period 给足，别让健康检查把正常启动判成失败
HEALTHCHECK --start-period=240s --interval=30s --timeout=10s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/', timeout=8)" || exit 1

EXPOSE 8000 8765
CMD ["python", "-u", "servev1.py"]
