"""云端大模型适配层 —— 可切换多家，全部走 OpenAI 兼容的 /chat/completions。

只依赖标准库（urllib），不装 SDK，换平台只改环境变量：

    set LLM_PROVIDER=zhipu          # zhipu / bailian / siliconflow / custom
    set LLM_API_KEY=xxxxxxxx        # 你的 key
    set LLM_MODEL=glm-4-flash       # 可留空用该平台默认模型

自定义 OpenAI 兼容端点：

    set LLM_PROVIDER=custom
    set LLM_BASE_URL=https://your.endpoint/v1
    set LLM_MODEL=your-model
    set LLM_API_KEY=xxxx

命令行冒烟：python llm_cloud.py "你好，你是谁"
"""

import json
import os
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# 平台预设：都是 OpenAI 兼容格式
# ---------------------------------------------------------------------------
PRESETS = {
    "zhipu": {
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "model": "glm-4-flash",
        "note": "智谱，glm-4-flash 官方永久免费不限量",
    },
    "bailian": {
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "model": "qwen-plus",
        "note": "阿里百炼，Qwen 开源系列官方托管，新用户送大量免费额度",
    },
    "siliconflow": {
        "base_url": "https://api.siliconflow.cn/v1",
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "note": "硅基流动，9B 以下模型永久免费",
    },
    "deepseek": {
        "base_url": "https://api.deepseek.com/v1",
        "model": "deepseek-chat",
        "note": "DeepSeek，新用户送免费额度",
    },
}


class LLMError(Exception):
    pass


# 配置文件（同目录 llm_config.json）：填了就用，改完下次调用即生效，不用重启服务。
# 优先级：环境变量 > 配置文件 > 平台预设。
CFG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "llm_config.json")
_FILE_CFG = {"mtime": 0, "data": {}}


def _file_config():
    """按 mtime 增量读取配置文件，避免每次调用都读盘。"""
    try:
        mt = os.path.getmtime(CFG_PATH)
    except OSError:
        return {}
    if mt != _FILE_CFG["mtime"]:
        try:
            with open(CFG_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f) or {}
            _FILE_CFG["data"] = {k: v for k, v in raw.items()
                                 if not str(k).startswith("_")}
        except Exception:  # noqa: BLE001
            _FILE_CFG["data"] = {}
        _FILE_CFG["mtime"] = mt
    return _FILE_CFG["data"]


def _config():
    fc = _file_config()
    provider = (os.environ.get("LLM_PROVIDER") or fc.get("provider") or "zhipu").strip().lower()
    preset = PRESETS.get(provider, {})
    base = (os.environ.get("LLM_BASE_URL") or fc.get("base_url")
            or preset.get("base_url") or "").rstrip("/")
    model = os.environ.get("LLM_MODEL") or fc.get("model") or preset.get("model") or ""
    key = os.environ.get("LLM_API_KEY") or fc.get("api_key") or ""
    return provider, base, model, key


def config_path():
    return CFG_PATH


def is_configured():
    _, base, model, key = _config()
    return bool(base and model and key)


def describe():
    provider, base, model, key = _config()
    return {
        "provider": provider,
        "base_url": base,
        "model": model,
        "key_set": bool(key),
        "ready": bool(base and model and key),
        "presets": {k: v["note"] for k, v in PRESETS.items()},
    }


def chat(messages, tools=None, temperature=0.7, max_tokens=400, timeout=25.0):
    """发一轮对话。返回 {"content": str, "tool_calls": [{"name":..,"arguments":dict}]}。

    tools 走 OpenAI function calling；模型不支持时会回退到纯文本，由上层自行兜底解析。
    """
    provider, base, model, key = _config()
    if not (base and model and key):
        raise LLMError("未配置大模型：需要 LLM_BASE_URL / LLM_MODEL / LLM_API_KEY")

    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "stream": False,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        base + "/chat/completions",
        data=data,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            j = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", "ignore")[:400]
        except Exception:  # noqa: BLE001
            pass
        raise LLMError(f"HTTP {e.code} from {provider}: {body}") from e
    except Exception as e:  # noqa: BLE001
        raise LLMError(f"请求失败（{provider}）：{e}") from e

    try:
        msg = j["choices"][0]["message"]
    except (KeyError, IndexError):
        raise LLMError(f"响应格式异常：{json.dumps(j, ensure_ascii=False)[:300]}")

    calls = []
    for c in (msg.get("tool_calls") or []):
        fn = (c or {}).get("function") or {}
        name = fn.get("name") or ""
        raw = fn.get("arguments") or "{}"
        try:
            args = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:  # noqa: BLE001
            args = {}
        calls.append({"name": name, "arguments": args, "id": (c or {}).get("id")})

    # raw_tool_calls 保留原始结构，供两阶段流程构造 role="tool" 消息（需要 id）
    return {"content": (msg.get("content") or "").strip(), "tool_calls": calls,
            "raw_tool_calls": msg.get("tool_calls") or [],
            "provider": provider, "model": model}


if __name__ == "__main__":
    import sys

    d = describe()
    print("当前配置：", json.dumps(d, ensure_ascii=False, indent=2))
    if not d["ready"]:
        print("\n未配置 LLM_API_KEY，无法实际调用。配置方法见本文件顶部注释。")
        sys.exit(0)
    text = sys.argv[1] if len(sys.argv) > 1 else "你好"
    r = chat([{"role": "user", "content": text}])
    print("\n回复：", r["content"])
