"""机器狗"大脑"编排层：把一句话变成 说话 + 表情 + 动作。

    文本(ASR) -> 提示词 + 能力清单 -> 云端大模型 -> 结构化决策 -> 已有下行链路

设计要点：
1. 动作能力走 OpenAI function calling（等价于 MCP 的 tools 层，但零额外进程）；
   模型不支持 tools 时自动降级为"提示词 + 从文本里抠 JSON"。
2. 三重保险：动作名白名单校验、表情白名单校验、整体异常兜底。
   最坏情况只是狗不动或只说话，绝不会乱动或卡住。
3. 本身不依赖 servev1，执行动作靠注入的 actors 回调，可单独测试。

命令行冒烟（不接真实下行）：python brain.py "坐下"
"""

import json
import os
import re
import threading
import time
from collections import deque

import llm_cloud

DEFAULT_CAPS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "dog_actions.json")


# 模型不走 tool_calls 时，会把工具意图写成文本（如 "showface sad"），念出来很出戏
_TOOL_LEAK = re.compile(
    r"(?i)\b(?:show[_\s]?face|perform[_\s]?action|direct[_\s]?answer|"
    r"tool[_\s]?call|function)\b"
    r"\s*[:(=]?\s*[\"']?[A-Za-z0-9_\-]*[\"']?\s*[)\]】]?",
    re.UNICODE)   # 参数部分只吃 ASCII，别把后面的中文台词一起吃掉
# 整段是 JSON 的残留
_JSON_ONLY = re.compile(r"^\s*\{.*\}\s*$", re.S)


def _clean_reply(text, max_chars):
    """清洗播报文本：去掉换行、括号内容、常见标记，避免 TTS 念出奇怪符号。"""
    if not text:
        return ""
    t = str(text).replace("\n", " ").replace("\r", " ")
    if _JSON_ONLY.match(t):                          # 整段就是 JSON，等于没说话
        return ""
    t = re.sub(r"[（(][^）)]*[）)]", "", t)          # 去掉括号补充说明
    t = re.sub(r"[*`#<>\[\]{}|~_]", "", t)          # 去掉 Markdown 标记
    t = _TOOL_LEAK.sub(" ", t)                       # 去掉工具名泄漏（showface 等）
    t = re.sub(r"[\U0001F000-\U0001FAFF\u2600-\u27BF\uFE0F]", "", t)  # emoji，TTS 念不了
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"[ \t]+([，。！？、；：])", r"\1", t)  # 收掉标点前多余空格
    t = t.strip("\"'“”‘’")
    if t.endswith(("...", "…")):        # 生成被长度截断，半句话念出来很怪
        i = max(t.rfind("。"), t.rfind("！"), t.rfind("？"))
        if i > 0:
            t = t[:i + 1]
    limit = int(max_chars * 1.6)                     # 硬截断，防止模型话痨拖慢 TTS
    if len(t) > limit:
        cut = t[:limit]
        for p in ("。", "！", "？", "，", "、"):
            i = cut.rfind(p)
            if i > limit * 0.5:
                return re.sub(r"^[，。、；：！？\s]+", "", cut[:i + 1])
        return re.sub(r"^[，。、；：！？\s]+", "", cut)
    return re.sub(r"^[，。、；：！？\s]+", "", t)


class DogBrain:
    def __init__(self, caps_path=None, actors=None, log=print):
        self.caps_path = caps_path or DEFAULT_CAPS
        self.actors = actors or {}
        self.log = log
        self.lock = threading.Lock()
        self.histories = {}
        self.caps = self._load_caps()

    # ---------------- 能力清单 ----------------
    def _load_caps(self):
        try:
            with open(self.caps_path, "r", encoding="utf-8") as f:
                caps = json.load(f)
        except Exception as e:  # noqa: BLE001
            print(f"[brain] 能力清单读取失败（{e}），用最小内置清单兜底")
            caps = {}
        caps.setdefault("persona", "你是一只活泼的机器狗，说话简短。")
        caps.setdefault("rules", ["回复 30 字以内中文口语"])
        caps.setdefault("max_reply_chars", 30)
        caps.setdefault("history_turns", 6)
        caps.setdefault("faces", ["happy", "angry", "sad", "joy"])
        caps.setdefault("board_faces", [])   # 板端全部表情名，只用来清洗台词，不给模型当选项
        caps.setdefault("actions", [])
        return caps

    def reload(self):
        self.caps = self._load_caps()

    # ---------------- 对话历史（调试台用） ----------------
    def history(self, device_id, limit=None):
        with self.lock:
            h = list(self.histories.get(device_id) or [])
        return h[-limit:] if limit else h

    def clear_history(self, device_id):
        with self.lock:
            n = len(self.histories.get(device_id) or [])
            self.histories.pop(device_id, None)
        return n

    def probe(self, timeout=15.0):
        """最小连通自检：不带工具问一句，验证 Key / 端点 / 模型名是否可用。"""
        t0 = time.time()
        try:
            r = llm_cloud.chat(
                [{"role": "system", "content": "你只回复两个字，不要任何标点。"},
                 {"role": "user", "content": "收到请回复：收到"}],
                tools=None, max_tokens=16, timeout=timeout)
            return {"ok": True, "latency_ms": int((time.time() - t0) * 1000),
                    "preview": (r.get("content") or "").strip()[:60],
                    "provider": r.get("provider"), "model": r.get("model")}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e),
                    "latency_ms": int((time.time() - t0) * 1000)}

    def action_names(self):
        return [a.get("name") for a in self.caps.get("actions", []) if a.get("name")]

    def repair_reply(self, messages, result, failed, err=""):
        """动作没能做成时，把失败结果写回对话再要一句台词。

        学自 xiaozhi-server 的 REQLLM：assistant(tool_calls) → tool(失败结果) → 再问一次。
        只在失败时触发，正常情况不额外多一次模型调用。
        """
        try:
            msgs = list(messages)
            msgs[0] = {"role": "system", "content": self.system_prompt(force_plain="repair")}
            raw = result.get("raw_tool_calls") or []
            if raw:
                msgs.append({"role": "assistant",
                             "content": result.get("content") or "",
                             "tool_calls": raw})
                for c in raw:
                    msgs.append({"role": "tool", "tool_call_id": c.get("id"),
                                 "content": f"动作执行失败：{err or '设备未响应'}"})
            else:
                msgs.append({"role": "user",
                             "content": f"（动作 {'、'.join(failed)} 没能做成：{err or '设备未响应'}）"
                                        f"请重新回应主人一句。"})
            r2 = llm_cloud.chat(msgs, tools=None, temperature=0.7, max_tokens=120)
        except Exception as e:  # noqa: BLE001
            self.log(f"[brain] 补话失败，用固定兜底：{e}")
            return ""
        return self._strip_names(
            _clean_reply(r2.get("content") or "", int(self.caps.get("max_reply_chars", 30))))

    def _strip_names(self, t):
        """去掉正文里混入的动作名/表情名（如 "bark 汪汪！"），TTS 不该念出这些标识符。"""
        if not t:
            return ""
        names = [a.get("name") for a in self.caps.get("actions", []) if a.get("name")]
        # 表情名也要剥：board_faces 里的 listening/thinking 等由服务端自己切，
        # 万一模型把它写进台词（"我现在是 thinking 状态"），念出来同样很出戏
        names += [f for f in self.caps.get("faces", []) if f]
        names += [f for f in self.caps.get("board_faces", []) if f]
        for n in sorted(set(names), key=len, reverse=True):
            t = re.sub(r"(?i)(?<![一-龥\w])" + re.escape(n) + r"(?![一-龥\w])", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        t = re.sub(r"[ \t]+([，。！？、；：])", r"\1", t).strip()
        return re.sub(r"^[，。、；：！？\s]+", "", t)

    def fallback_reply(self, actions, face):
        """模型只调工具、正文空着时的兜底话术，避免狗变成哑巴。"""
        for a in actions or []:
            for item in self.caps.get("actions", []):
                if item.get("name") == a:
                    return item.get("say") or f"好的，{item.get('desc') or a}！"
        if face:
            return (self.caps.get("face_say") or {}).get(face) or ""
        return ""

    # ---------------- 提示词 ----------------
    def system_prompt(self, json_mode=False, force_plain=False):
        if force_plain:
            if force_plain == "repair":
                # 动作没做成时的补话：要承认没做成，别装作做到了
                return "\n".join([self.caps.get("persona", ""),
                                  "你要做的动作没能做成。用一句简短的中文口语向主人说明并带过，"
                                  "不要提工具名和动作名，不要解释技术原因，不要假装已经做了。"])
            return "\n".join([self.caps.get("persona", ""),
                              "只输出一句对主人说的中文口语，不要调用工具，不要解释，不要标点以外的符号。"])
        acts = "、".join(f"{a.get('name')}({a.get('desc', '')})"
                        for a in self.caps.get("actions", []))
        faces = "、".join(self.caps.get("faces", []))
        lines = [self.caps.get("persona", "")]
        lines.append("可用动作：" + (acts or "（暂无）"))
        lines.append("可用表情：" + (faces or "（暂无）"))
        lines.extend(self.caps.get("rules", []))
        if json_mode:
            lines.append(
                '请只输出一个 JSON 对象，不要任何解释文字：'
                '{"reply":"要说的中文短句","face":"表情名或null","action":"动作名或null"}'
                '。其中 reply 必填，不能为空。')
        else:
            # GLM / Qwen 调工具时正文常常为空，所以台词走工具的必填参数，不靠正文
            lines.append(
                "硬性要求：每次都必须调用且只调用一个工具。"
                "要做动作就调 perform_action，只想换表情就调 show_face，"
                "两者都不需要就调 direct_answer。"
                "你要说的话一律写在工具的 response 参数里（必填），"
                "不要把台词写在正文里——正文会被忽略。")
        return "\n".join(x for x in lines if x)

    def tools(self, include_direct=True):
        """function calling 声明。

        学自 xiaozhi-server 的两个关键设计：
        1. 每个工具都强制必填 response——逼模型在决定动作的同时就把台词交出来，
           而不是指望它另写一段正文（GLM 等模型调工具时正文常常为空）。
        2. direct_answer 是个虚拟工具：把"调不调工具"的二选一变成"调哪个"的多选，
           模型永远有东西可调，正文就不会空。只在第一轮注入，避免两阶段时循环调用。
        """
        names = self.action_names()
        faces = self.caps.get("faces", [])
        out = []
        if names:
            desc = "让机器狗做一个动作。name 只能取这些值：" + "、".join(
                f"{a.get('name')}({a.get('desc', '')})"
                for a in self.caps.get("actions", []))
            out.append({
                "type": "function",
                "function": {
                    "name": "perform_action",
                    "description": desc,
                    "parameters": {"type": "object",
                                   "properties": {
                                       "name": {"type": "string", "enum": names},
                                       "response": {"type": "string",
                                                    "description": "做这个动作时你要对主人说的话，必填"}},
                                   "required": ["name", "response"]},
                },
            })
        if faces:
            out.append({
                "type": "function",
                "function": {
                    "name": "show_face",
                    "description": "切换机器狗的表情。name 只能取这些值：" + "、".join(faces),
                    "parameters": {"type": "object",
                                   "properties": {
                                       "name": {"type": "string", "enum": faces},
                                       "response": {"type": "string",
                                                    "description": "切这个表情时你要对主人说的话，必填"}},
                                   "required": ["name", "response"]},
                },
            })
        if include_direct:
            out.append({
                "type": "function",
                "function": {
                    "name": "direct_answer",
                    "description": "不需要做动作、也不需要换表情时，用这个直接回复主人。",
                    "parameters": {"type": "object",
                                   "properties": {"response": {"type": "string",
                                                               "description": "你要对主人说的完整回复，必填"}},
                                   "required": ["response"]},
                },
            })
        return out

    # ---------------- 解析 ----------------
    @staticmethod
    def _json_from_text(text):
        m = re.search(r"\{[^{}]*\}", text or "")
        if not m:
            return {}
        try:
            return json.loads(m.group(0))
        except Exception:  # noqa: BLE001
            return {}

    def parse(self, result):
        """从大模型返回里拆出 reply / face / action，并做白名单校验。"""
        face = None
        actions = []
        rejected = []          # 被白名单拦下的调用，用于后面补一句拒绝话术
        used_tools = False
        reply = ""
        # 台词候选：模型可能同时调多个工具，按"动作 > 表情 > 直接回答"取第一句非空的
        cand = {"action": "", "face": "", "direct": ""}

        for c in result.get("tool_calls") or []:
            name = c.get("name")
            args = c.get("arguments") or {}
            if isinstance(args, str):          # 有些平台回传的是 JSON 字符串
                try:
                    args = json.loads(args)
                except Exception:  # noqa: BLE001
                    args = {}
            if not isinstance(args, dict):
                args = {}
            resp = str(args.get("response") or "").strip()
            if name == "show_face":
                used_tools = True
                if args.get("name") in self.caps.get("faces", []):
                    face = args["name"]
                    cand["face"] = cand["face"] or resp
                else:
                    rejected.append(args.get("name"))
            elif name == "perform_action":
                used_tools = True
                if args.get("name") in self.action_names():
                    actions.append(args["name"])
                    cand["action"] = cand["action"] or resp
                else:
                    rejected.append(args.get("name"))
            elif name == "direct_answer":
                used_tools = True
                cand["direct"] = cand["direct"] or resp
            else:
                # 模型编了我们没声明的工具（如 get_weather）：当"做不到"处理，
                # 但它要是顺手给了台词，那句台词还是能用的
                used_tools = True
                rejected.append(name)
                cand["direct"] = cand["direct"] or resp

        if used_tools:
            # 台词来自工具的 response 必填参数，这才是它不会为空的原因
            reply = cand["action"] or cand["face"] or cand["direct"]

        if not reply:
            reply = result.get("content") or ""
            if not used_tools:
                # 降级路径：模型没走工具，就从文本里抠 JSON；抠不到就当纯文本回复
                j = self._json_from_text(reply)
                if j:
                    reply = j.get("reply") or ""
                    if j.get("face") in self.caps.get("faces", []):
                        face = j["face"]
                    if j.get("action") in self.action_names():
                        actions.append(j["action"])

        clean = _clean_reply(reply, int(self.caps.get("max_reply_chars", 30)))
        return {"reply": self._strip_names(clean),
                "face": face, "actions": actions, "rejected": rejected}

    # ---------------- 调试信息（给调试台看） ----------------
    def _debug(self, messages, result, notes, t0, dry):
        calls = result.get("tool_calls") or []
        return {
            "dry": bool(dry),
            "latency_ms": int((time.time() - t0) * 1000),
            "path": ("tool_calls" if calls else "纯文本"),
            "system_prompt": (messages[0] or {}).get("content", "") if messages else "",
            "messages": messages,
            "raw_content": result.get("content") or "",
            "raw_tool_calls": calls,
            "notes": notes,
            "tools_declared": [t["function"]["name"] for t in self.tools()],
        }

    # ---------------- 主流程 ----------------
    def handle(self, device_id, text, dry=False):
        """输入一句用户话，输出决策并执行（通过注入的 actors）。

        dry=True 为"预演"：照常问模型、照常解析，但不下发任何动作/播报，也不写对话历史。
        调试台靠它安全地看模型到底给了什么，不会真的让狗动起来。
        """
        text = (text or "").strip()
        if not text:
            return {"ok": False, "error": "empty input"}
        if not llm_cloud.is_configured():
            return {"ok": False, "error": "LLM 未配置（缺 LLM_API_KEY 等）"}

        notes = []                     # 走过的兜底路径，调试台会原样显示
        t0 = time.time()
        with self.lock:
            hist = self.histories.setdefault(
                device_id, deque(maxlen=int(self.caps.get("history_turns", 6)) * 2))
            messages = [{"role": "system", "content": self.system_prompt()}]
            messages.extend(list(hist))
            messages.append({"role": "user", "content": text})

        try:
            # max_tokens 压到 160：狗要说短句，放开了会同答一大段，TTS 跟着被拖到好几秒
            result = llm_cloud.chat(messages, tools=self.tools() or None, max_tokens=160)
        except Exception as e:  # noqa: BLE001
            # 平台不支持 tools（400）时，降级为提示词 + JSON
            self.log(f"[brain] 带 tools 调用失败，降级为 JSON 模式：{e}")
            notes.append(f"带 tools 调用失败，已降级为 JSON 模式（{e}）")
            try:
                messages[0] = {"role": "system", "content": self.system_prompt(json_mode=True)}
                result = llm_cloud.chat(messages, tools=None, max_tokens=160)
            except Exception as e2:  # noqa: BLE001
                self.log(f"[brain] 大模型调用失败：{e2}")
                return {"ok": False, "error": str(e2)}

        d = self.parse(result)
        if not d["reply"] and not (result.get("tool_calls") or []):
            # 模型这轮既没调工具也没说话（偶发）：改成纯文本模式再要一次。
            # 比直接扔一句固定兜底话术聪明，且只在异常时多花这一秒。
            try:
                m2 = list(messages)
                m2[0] = {"role": "system", "content": self.system_prompt(force_plain=True)}
                r2 = llm_cloud.chat(m2, tools=None, max_tokens=160)
                again = self.parse(r2)
                if again["reply"]:
                    d["reply"] = again["reply"]
                    notes.append("首轮没给台词，已用纯文本模式重试拿到")
                    self.log("[brain] 首轮无输出，纯文本重试拿到台词")
            except Exception as e:  # noqa: BLE001
                self.log(f"[brain] 纯文本重试失败：{e}")

        if not d["reply"] and (d["actions"] or d["face"]):
            d["reply"] = self.fallback_reply(d["actions"], d["face"])
            if d["reply"]:
                notes.append(f"模型正文为空，用了动作自带兜底话术：{d['reply']}")
                self.log(f"[brain] 模型正文为空，改用兜底话术：{d['reply']!r}")
        if not d["reply"] and d.get("rejected"):
            # 模型叫了个不存在的动作/工具（如"翻跟头"、get_weather），被白名单拦下；
            # 这时候必须说话，否则狗会莫名其妙沉默，主人以为它坏了。
            d["reply"] = self.caps.get("reject_reply") or "这个我还不会诶，换一个吧"
            notes.append(f"模型编了白名单外的动作/工具 {d['rejected']}，已拦下并改用拒绝话术")
            self.log(f"[brain] 调用 {d['rejected']} 不在白名单，改用拒绝话术")
        if not d["reply"]:
            # 最后一道防线：语音设备上沉默是致命的，宁可说句废话也别哑巴
            d["reply"] = self.caps.get("empty_reply") or "嗯？我没听清呢，再说一遍？"
            notes.append("模型没给任何台词，用通用兜底话术")
            self.log("[brain] 模型没给任何台词，用通用兜底")

        if not dry:
            with self.lock:
                hist.append({"role": "user", "content": text})
                # 没说话也要占位，否则历史里出现连续两条 user，下一轮模型会串味
                hist.append({"role": "assistant",
                             "content": d["reply"] or "（没说话，只是做了个动作）"})

        executed = {"face": None, "actions": [], "speak": None}
        if dry:
            out = {"ok": True, "reply": d["reply"], "face": d["face"],
                   "actions": d["actions"], "rejected": d.get("rejected") or [],
                   "executed": executed, "model": result.get("model"),
                   "provider": result.get("provider"),
                   "debug": self._debug(messages, result, notes, t0, dry)}
            self.log(f"[brain·预演] {text!r} -> 说：{d['reply']!r}"
                     f" 表情：{d['face']} 动作：{d['actions']}")
            return out

        if d["face"] and self.actors.get("face"):
            try:
                r = self.actors["face"](device_id, d["face"])
                executed["face"] = d["face"] if (r or {}).get("ok", True) else None
                if not (r or {}).get("ok", True):
                    self.log(f"[brain] 表情下发失败：{(r or {}).get('error')}")
            except Exception as e:  # noqa: BLE001
                self.log(f"[brain] 表情执行异常：{e}")

        # 先起播报再下发动作：播放是异步排队、立刻返回；动作下发可能同步等板子回执。
        # 顺序反过来会变成"狗先做完动作才开口"。
        if d["reply"] and self.actors.get("speak"):
            try:
                r = self.actors["speak"](device_id, d["reply"])
                executed["speak"] = d["reply"] if (r or {}).get("ok", True) else None
                if not (r or {}).get("ok", True):
                    self.log(f"[brain] 播报失败：{(r or {}).get('error')}")
            except Exception as e:  # noqa: BLE001
                self.log(f"[brain] 播报异常：{e}")

        failed, errs = [], []
        for a in d["actions"][:2]:                      # 最多两个动作，防止模型刷屏
            if not self.actors.get("action"):
                break
            try:
                r = self.actors["action"](device_id, a)
                if (r or {}).get("ok", True):
                    executed["actions"].append(a)
                else:
                    failed.append(a)
                    errs.append(str((r or {}).get("error") or "被拒绝"))
                    self.log(f"[brain] 动作 {a} 被拒：{(r or {}).get('error')}"
                             f" {(r or {}).get('hint', '')}")
            except Exception as e:  # noqa: BLE001
                failed.append(a)
                errs.append(str(e))
                self.log(f"[brain] 动作执行异常：{e}")

        # 动作没做成：把失败结果喂回对话再要一句台词，避免狗"说到做不到"还不吭声
        if failed and self.caps.get("repair_on_failure", True) and self.actors.get("speak"):
            fix = self.repair_reply(messages, result, failed, "；".join(errs))
            if fix:
                try:
                    self.actors["speak"](device_id, fix)
                    executed["repair"] = fix
                    notes.append(f"动作 {failed} 没做成，补了一句：{fix}")
                    self.log(f"[brain] 动作 {failed} 没做成，补一句：{fix!r}")
                except Exception as e:  # noqa: BLE001
                    self.log(f"[brain] 补话播报异常：{e}")

        out = {"ok": True, "reply": d["reply"], "face": d["face"],
               "actions": d["actions"], "rejected": d.get("rejected") or [],
               "executed": executed,
               "model": result.get("model"), "provider": result.get("provider"),
               "debug": self._debug(messages, result, notes, t0, dry)}
        self.log(f"[brain] {text!r} -> 说：{d['reply']!r}"
                 f" 表情：{d['face']} 动作：{d['actions']}")
        return out


if __name__ == "__main__":
    import sys

    b = DogBrain()
    print("动作清单：", b.action_names())
    print("配置：", json.dumps(llm_cloud.describe(), ensure_ascii=False))
    if not llm_cloud.is_configured():
        print("\n未配置 API Key，只演示解析层（用假返回跑一遍白名单）：")
        fake = {"content": "好的主人，我这就坐下！",
                "tool_calls": [{"name": "perform_action", "arguments": '{"name":"sit"}'},
                               {"name": "show_face", "arguments": '{"name":"happy"}'},
                               {"name": "perform_action", "arguments": '{"name":"翻跟头"}'}]}
        print(json.dumps(b.parse(fake), ensure_ascii=False))
        sys.exit(0)
    print(json.dumps(b.handle("dog-001", sys.argv[1] if len(sys.argv) > 1 else "坐下"),
                     ensure_ascii=False, indent=2))
