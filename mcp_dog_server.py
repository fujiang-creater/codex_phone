"""机器狗 MCP Server —— 让 WorkBuddy / Claude Desktop 这类 AI 客户端直接对话控制机器狗。

它本身不含业务逻辑，全部通过 HTTP 调用已经在跑的 servev1.py（默认 :8000），
所以**必须先启动服务端**，MCP 才能用。

启动方式（服务端先跑起来）：
    python mcp_dog_server.py            # stdio 传输，供 MCP 客户端拉起

环境变量：
    DOG_SERVER=http://127.0.0.1:8000    # 服务端地址
    DOG_SERVER_TOKEN=dev-token-change-me
    DOG_BRAIN_DEVICE=dog-001            # 默认设备 id，工具里可临时覆盖

在 WorkBuddy / Claude Desktop 里注册（mcpServers）：
    {
      "mcpServers": {
        "dog": {
          "command": "F:/Users/lenovo/miniconda3/envs/dog_voice/python.exe",
          "args": ["F:/project/yanjiushenglunwen/yanjiusheng/机器狗项目/server/mcp_dog_server.py"]
        }
      }
    }
"""

import json
import os
import urllib.error
import urllib.request

try:                                    # mcp 2.x：FastMCP 改名 MCPServer
    from mcp.server.mcpserver import MCPServer as _Server
except ImportError:                     # mcp 1.x 兜底
    from mcp.server.fastmcp import FastMCP as _Server

BASE = os.environ.get("DOG_SERVER", "http://127.0.0.1:8000").rstrip("/")
TOKEN = os.environ.get("DOG_SERVER_TOKEN", "dev-token-change-me")
DEFAULT_DEVICE = os.environ.get("DOG_BRAIN_DEVICE", "dog-001")
CAPS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dog_actions.json")

mcp = _Server("机器狗控制台")


# --------------------------------------------------------------------------- HTTP
def _req(method, path, body=None):
    url = BASE + path
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer " + TOKEN})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", "ignore")[:300]
        except Exception:  # noqa: BLE001
            pass
        return {"ok": False, "error": f"HTTP {e.code}", "detail": detail}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"连不上服务端（{BASE}）：{e}"}


def _caps():
    try:
        with open(CAPS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return {"actions": [], "faces": []}


def _fmt(res):
    if res.get("ok"):
        return "成功：" + json.dumps({k: v for k, v in res.items() if k != "ok"},
                                    ensure_ascii=False)
    return "失败：" + res.get("error", "未知错误") + (
        " | " + str(res.get("hint") or res.get("detail") or "")
        if (res.get("hint") or res.get("detail")) else "")


# --------------------------------------------------------------------------- 工具
@mcp.tool()
def dog_actions() -> str:
    """列出机器狗能做的所有动作名和表情名。想让狗做动作前先调这个，避免用错名字。"""
    c = _caps()
    acts = "、".join(f"{a.get('name')}({a.get('desc', '')})" for a in c.get("actions", []))
    return f"动作：{acts or '（无）'}\n表情：{'、'.join(c.get('faces', [])) or '（无）'}"


@mcp.tool()
def dog_status(device_id: str = "") -> str:
    """查询机器狗当前状态：是否在线、当前状态、正在执行的动作、播放队列长度。"""
    did = device_id or DEFAULT_DEVICE
    res = _req("GET", "/api/v1/devices/state")
    d = ((res.get("devices") or {}).get(did)) if res.get("ok") else None
    if not d:
        return f"设备 {did} 暂无状态（可能未连接）：{json.dumps(res, ensure_ascii=False)}"
    cur = (res.get("current") or {}).get(did)
    q = (res.get("queue") or {}).get(did) or 0
    return (f"设备 {did}：状态 {d.get('state')}，在线 {d.get('ws_alive')}，"
            f"上报年龄 {d.get('age')}s，当前动作 {cur.get('command') if cur else '无'}，"
            f"播放队列 {q}")


@mcp.tool()
def dog_speak(text: str, device_id: str = "") -> str:
    """让机器狗用语音说出一段中文（edge-tts 合成后下发到板子播放）。建议 30 字以内。"""
    did = device_id or DEFAULT_DEVICE
    return _fmt(_req("POST", f"/api/v1/devices/{did}/play", {"text": text}))


@mcp.tool()
def dog_action(name: str, device_id: str = "") -> str:
    """让机器狗做一个动作。name 必须来自 dog_actions 列出的动作名，不存在的名字会被服务端拒绝。"""
    did = device_id or DEFAULT_DEVICE
    names = [a.get("name") for a in _caps().get("actions", [])]
    if names and name not in names:
        return f"动作名 {name!r} 不在清单里，可用：{'、'.join(names)}"
    return _fmt(_req("POST", f"/api/v1/devices/{did}/command",
                     {"command": "action.run", "args": {"name": name}}))


@mcp.tool()
def dog_face(name: str, device_id: str = "") -> str:
    """切换机器狗屏幕上的表情。name 取 happy / angry / sad / joy 之一。"""
    did = device_id or DEFAULT_DEVICE
    faces = _caps().get("faces", [])
    if faces and name not in faces:
        return f"表情名 {name!r} 不在清单里，可用：{'、'.join(faces)}"
    return _fmt(_req("POST", f"/api/v1/devices/{did}/face", {"name": name}))


@mcp.tool()
def dog_chat(text: str, device_id: str = "") -> str:
    """和机器狗对话：把一句话交给云端大模型，它会自己决定回答什么、做什么动作、换什么表情。
    需要服务端已开启大脑层（DOG_BRAIN=1）并配好 LLM_API_KEY。"""
    did = device_id or DEFAULT_DEVICE
    res = _req("POST", "/api/v1/brain/say",
               {"device_id": did, "text": text, "wait": True})
    if not res.get("ok"):
        return _fmt(res)
    ex = res.get("executed") or {}
    parts = [f"狗说：{res.get('reply') or '（没说话）'}"]
    if ex.get("face"):
        parts.append(f"表情：{ex['face']}")
    if ex.get("actions"):
        parts.append(f"动作：{'、'.join(ex['actions'])}")
    return "；".join(parts) + f"（模型 {res.get('provider')}/{res.get('model')}）"


@mcp.tool()
def dog_stop(device_id: str = "") -> str:
    """让机器狗立刻停止当前动作和语音播放（急停）。"""
    did = device_id or DEFAULT_DEVICE
    r1 = _req("POST", f"/api/v1/devices/{did}/audio/abort", {})
    r2 = _req("POST", f"/api/v1/devices/{did}/command",
              {"command": "action.stop", "args": {}})
    return f"停止语音：{'成功' if r1.get('ok') else r1.get('error')}；" \
           f"停止动作：{'成功' if r2.get('ok') else r2.get('error')}"


if __name__ == "__main__":
    mcp.run()
