"""
Cloud LINE Webhook Server
Runs on free cloud hosting (Render / Vercel / Railway) with 100% permanent static URL.
Receives LINE events, verifies signatures, and manages interactive ordering state.
"""

from __future__ import annotations
import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional
import requests
from fastapi import FastAPI, Header, HTTPException, Request, Response

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
app = FastAPI(title="Cloud LINE Webhook Assistant")

# Configuration from environment variables
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "aFSMLCwvNQN1EX9moME08ukw//AIa4X4zZ3llkLTY6TLxWGp823zyATHm+pzuE0dTD5xPrxhYGgcLODkor06T0xJRFdC6toIVXp+OvmDN0wGez+wr3/Dl/eLQLMz7Cu6BKBLUPPxfANbdUl7vkIb7gdB04t89/1O/w1cDnyilFU=")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "1f7b856d18abb948fa827a8a3acb80cd")
LINE_USER_ID = os.getenv("LINE_USER_ID", "U1f8fde04d26c2798361c804bec20c175")

# In-memory queues for bridging to local trading engine
PENDING_TASKS: List[Dict[str, Any]] = []
EXECUTED_RESULTS: List[Dict[str, Any]] = []


def verify_signature(body_bytes: bytes, signature: str) -> bool:
    if not LINE_CHANNEL_SECRET:
        return True
    hash_val = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body_bytes, hashlib.sha256).digest()
    expected_sig = base64.b64encode(hash_val).decode("utf-8")
    return hmac.compare_digest(expected_sig, signature)


def reply_line_message(reply_token: str, text: str) -> bool:
    if not LINE_CHANNEL_ACCESS_TOKEN or not reply_token:
        return False
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"}
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text}]}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=5)
        return r.status_code == 200
    except Exception as e:
        logging.error(f"Reply error: {e}")
        return False


@app.get("/")
@app.get("/health")
def health():
    return {
        "status": "online",
        "service": "Cloud LINE Webhook Assistant",
        "time": datetime.datetime.now().isoformat(),
        "pending_tasks_count": len(PENDING_TASKS)
    }


@app.post("/webhook")
async def handle_webhook(request: Request, x_line_signature: Optional[str] = Header(None)):
    body_bytes = await request.body()
    body_str = body_bytes.decode("utf-8")

    if x_line_signature and LINE_CHANNEL_SECRET:
        if not verify_signature(body_bytes, x_line_signature):
            logging.warning("Invalid LINE signature received.")
            raise HTTPException(status_code=400, detail="Invalid signature")

    try:
        data = json.loads(body_str)
    except Exception:
        return Response(content="OK", status_code=200)

    events = data.get("events", [])
    for ev in events:
        ev_type = ev.get("type")
        source = ev.get("source", {})
        user_id = source.get("userId", "")
        reply_token = ev.get("replyToken", "")

        if LINE_USER_ID and user_id != LINE_USER_ID:
            continue

        # 1. Postback Button Click (Confirm / Cancel)
        if ev_type == "postback":
            pb_data = ev.get("postback", {}).get("data", "")
            logging.info(f"Received Postback data: {pb_data}")
            params = dict(p.split("=", 1) for p in pb_data.split("&") if "=" in p)
            action = params.get("action")
            order_id = params.get("order_id", "")

            # Queue task for local Shioaji executor
            task_item = {
                "id": f"task_{int(time.time()*1000)}",
                "type": "POSTBACK",
                "action": action,
                "order_id": order_id,
                "reply_token": reply_token,
                "timestamp": time.time(),
                "status": "QUEUED"
            }
            PENDING_TASKS.append(task_item)
            
            # Temporary immediate ack or handled by local bridge
            if action == "CANCEL_ORDER":
                reply_line_message(reply_token, "👌 已取消該筆下單操作，未送出任何委託。")
            else:
                reply_line_message(reply_token, "⚡ 收到確認指令！正在連線永豐金證券執行委託，請稍候回報...")

        # 2. Text Message Command
        elif ev_type == "message":
            msg = ev.get("message", {})
            if msg.get("type") == "text":
                user_text = msg.get("text", "").strip()
                logging.info(f"Received text command: {user_text}")

                task_item = {
                    "id": f"task_{int(time.time()*1000)}",
                    "type": "COMMAND",
                    "text": user_text,
                    "reply_token": reply_token,
                    "timestamp": time.time(),
                    "status": "QUEUED"
                }
                PENDING_TASKS.append(task_item)

    return Response(content="OK", status_code=200)


@app.get("/api/tasks/poll")
def poll_tasks(secret_key: Optional[str] = None):
    """Local home PC polls pending tasks from cloud."""
    global PENDING_TASKS
    tasks = list(PENDING_TASKS)
    PENDING_TASKS.clear()
    return {"tasks": tasks}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
