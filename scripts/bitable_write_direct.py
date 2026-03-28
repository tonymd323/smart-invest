#!/usr/bin/env python3
"""直接调用飞书 Bitable API 写入记录（绕过 OpenClaw tool 层）"""
import json
import urllib.request
import os
import sys

CONFIG_FILE = "/root/.openclaw/openclaw.json"
PENDING_FILE = "/root/.openclaw/workspace/smart-invest/data/bitable_pending.json"
APP_TOKEN = "CvTRbdVyfa9PnMsnzIXcCSNmnnb"
TABLE_ID = "tbluSQrjOW0tppTP"
BASE_URL = "https://open.feishu.cn/open-apis"

def get_app_credentials():
    with open(CONFIG_FILE) as f:
        cfg = json.load(f)
    feishu = cfg.get('channels', {}).get('feishu', {})
    return feishu.get('appId'), feishu.get('appSecret')

def get_tenant_token(app_id, app_secret):
    payload = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode()
    req = urllib.request.Request(
        f"{BASE_URL}/auth/v3/tenant_access_token/internal",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    if data.get("code") == 0:
        return data["tenant_access_token"]
    raise Exception(f"Token error: {data}")

def batch_create(token, records):
    payload = json.dumps({"records": records}).encode()
    url = f"{BASE_URL}/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_create"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())

def main():
    try:
        with open(PENDING_FILE, 'r', encoding='utf-8') as f:
            records = json.load(f)
    except FileNotFoundError:
        print("No pending records"); return
    except json.JSONDecodeError as e:
        print(f"JSON error: {e}"); return

    if not records:
        print("No records"); return

    print(f"Writing {len(records)} records...")

    app_id, app_secret = get_app_credentials()
    token = get_tenant_token(app_id, app_secret)
    print(f"Got tenant token ✅")

    batch_size = 50
    success = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            result = batch_create(token, batch)
            code = result.get("code", -1)
            if code == 0:
                success += len(batch)
                print(f"  Batch {i//batch_size+1}: {len(batch)} ✅")
            else:
                print(f"  Batch {i//batch_size+1}: FAIL code={code} msg={result.get('msg','')[:100]}")
        except Exception as e:
            print(f"  Batch {i//batch_size+1}: ERROR {e}")

    print(f"\nDone: {success}/{len(records)}")
    if success == len(records):
        os.remove(PENDING_FILE)
        print("Cleaned up pending file ✅")

if __name__ == "__main__":
    main()
