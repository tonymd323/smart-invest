#!/usr/bin/env python3
"""通过 OpenClaw gateway API 直接调用 feishu_bitable_app_table_record 写入记录"""
import json
import sys
import urllib.request

GATEWAY_URL = "http://127.0.0.1:19001"
APP_TOKEN = "CvTRbdVyfa9PnMsnzIXcCSNmnnb"
TABLE_ID = "tbluSQrjOW0tppTP"
PENDING_FILE = "/root/.openclaw/workspace/smart-invest/data/bitable_pending.json"

def call_tool(tool_name, args):
    """通过 gateway 的 /run endpoint 调用工具"""
    payload = json.dumps({"tool": tool_name, "args": args}).encode('utf-8')
    req = urllib.request.Request(
        f"{GATEWAY_URL}/api/run",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        return {"error": str(e)}

def main():
    # 读取待写入记录
    try:
        with open(PENDING_FILE, 'r', encoding='utf-8') as f:
            records = json.load(f)
    except FileNotFoundError:
        print("No pending records found")
        return
    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        return

    if not records:
        print("No records to write")
        return

    print(f"Writing {len(records)} records to bitable...")
    
    # 分批写入 (每批最多50条)
    batch_size = 50
    success = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        result = call_tool("feishu_bitable_app_table_record", {
            "action": "batch_create",
            "app_token": APP_TOKEN,
            "table_id": TABLE_ID,
            "records": batch,
        })
        if result.get("success"):
            success += len(batch)
            print(f"  Batch {i//batch_size + 1}: {len(batch)} records ✅")
        else:
            print(f"  Batch {i//batch_size + 1}: FAILED - {result.get('error', result)[:200]}")

    print(f"\nDone: {success}/{len(records)} records written")
    
    # 写入成功后清理
    if success == len(records):
        import os
        os.remove(PENDING_FILE)
        print("Cleaned up pending file")

if __name__ == "__main__":
    main()
