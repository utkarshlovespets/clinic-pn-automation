import os
import csv
import sys
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
REGION = os.getenv("CLEVERTAP_REGION", "in1")
ACCOUNT_ID = os.getenv("CLEVERTAP_ACCOUNT_ID", "YOUR_ACCOUNT_ID")
PASSCODE = os.getenv("CLEVERTAP_PASSCODE", "YOUR_PASSCODE")
CAMPAIGN_ID = os.getenv("CLEVERTAP_CAMPAIGN_ID", "YOUR_CAMPAIGN_ID")

csv_path = os.path.join(os.path.dirname(__file__), "data", "test_users.csv")
url = f"https://{REGION}.api.clevertap.com/1/send/externaltrigger.json"

headers = {
    "X-CleverTap-Account-Id": ACCOUNT_ID,
    "X-CleverTap-Passcode": PASSCODE,
    "Content-Type": "application/json"
}

# --- PAYLOAD PREPARATION ---
raw_title = "{your pet}'s safe space is loading...⏳"
raw_body = "Supertails+ Clinic will be here in no time. Drop by soon with {your pet} for stress-free care by Fear-Free certified vets.💜"

def format_clevertap_text(text: str, pet_name: str, first_name: str) -> str:
    if not text:
        return text
    
    # Use CSV values if present, else fallback to standard CleverTap Liquid tags
    pet_name_replacement = pet_name if pet_name else "{{ Profile['Pet Name'] | default: 'your pet' }}"
    first_name_replacement = first_name if first_name else "{{ Profile['First Name'] | default: 'pet parent' }}"
    
    text = text.replace("{pet parent}", first_name_replacement)
    text = text.replace("{your pet}", pet_name_replacement)
    return text

users_processed = 0

try:
    with open(csv_path, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        
        # Normalize headers to lowercase to avoid case-sensitivity issues
        if reader.fieldnames:
            reader.fieldnames = [col.strip().lower() if col else '' for col in reader.fieldnames]
            
        for row in reader:
            email = row.get('email', '').strip()
            if not email:
                continue
                
            pet_name = row.get('pet_name', '').strip()
            first_name = row.get('first_name', '').strip()
            
            # Formulate personalized strings
            target_title = format_clevertap_text(raw_title, pet_name, first_name)
            target_body = format_clevertap_text(raw_body, pet_name, first_name)
            
            payload = {
                "to": {
                    "email": [email]
                },
                "campaign_id": CAMPAIGN_ID,
                "ExternalTrigger": {
                    "title": target_title,
                    "body": target_body,
                    "android_deeplink": "https://supertails.com/pages/supertails-clinic",
                    "ios_deeplink": "supertails-com/pages/supertails-clinic" #ignore this error for now
                }
            }

            # --- EXECUTION ---
            print(f"Triggering campaign for {email}...")
            try:
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                response.raise_for_status()
                
                data = response.json()
                if data.get("status") == "success":
                    print(f"✅ Success! Message: {data.get('message')}")
                    users_processed += 1
                else:
                    print(f"❌ CleverTap Error for {email}: {data.get('error')}")
            
            except requests.exceptions.HTTPError as http_err:
                print(f"❌ HTTP Error for {email}: {http_err.response.status_code} - {http_err.response.text}")
            except Exception as e:
                print(f"❌ Script Error for {email}: {e}")

except FileNotFoundError:
    print(f"❌ Error: {csv_path} not found. Aborting...")
    sys.exit(1)

if users_processed == 0:
    print("❌ Error: No valid emails processed from test_users.csv.")
    sys.exit(1)