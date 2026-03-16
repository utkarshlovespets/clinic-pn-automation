import os
import json
import requests
from pathlib import Path
from dotenv import dotenv_values

def main():
    script_dir = Path(__file__).resolve().parent
    env = dotenv_values(script_dir / ".env")
    
    account_id = (env.get("CLEVERTAP_ACCOUNT_ID") or "").strip()
    passcode = (env.get("CLEVERTAP_PASSCODE") or "").strip()
    region = (env.get("CLEVERTAP_REGION") or "").strip()
    
    if not account_id or not passcode or not region:
        print("Error: Missing CLEVERTAP_ACCOUNT_ID, CLEVERTAP_PASSCODE, or CLEVERTAP_REGION in .env")
        return

    api_url = f"https://{region}.api.clevertap.com/1/targets/create.json"
    headers = {
        "X-CleverTap-Account-Id": account_id,
        "X-CleverTap-Passcode": passcode,
        "Content-Type": "application/json",
    }
    
    send_dir = script_dir / "send"
    if not send_dir.exists() or not send_dir.is_dir():
        print(f"Error: Directory '{send_dir}' does not exist. Creating it now...")
        send_dir.mkdir(parents=True, exist_ok=True)
        return
        
    json_files = list(send_dir.glob("*.json"))
    if not json_files:
        print("No JSON files found in the 'send' folder.")
        return
        
    print(f"Found {len(json_files)} payload(s) in 'send' folder.")
    
    for file_path in json_files:
        print(f"\nProcessing: {file_path.name}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
                
            response = requests.post(api_url, json=payload, headers=headers, timeout=60)
            
            try:
                response.raise_for_status()
                print(f"✅ Success! Response: {response.json()}")
            except requests.HTTPError as exc:
                body_text = response.text.strip()
                try:
                    parsed = response.json()
                    body_text = json.dumps(parsed, ensure_ascii=True)
                except ValueError:
                    pass
                print(f"❌ Failed: API error {response.status_code}: {body_text}")
                
        except json.JSONDecodeError as e:
            print(f"❌ Failed: Invalid JSON in file - {e}")
        except Exception as e:
            print(f"❌ Failed: Unexpected error - {e}")

if __name__ == "__main__":
    main()

