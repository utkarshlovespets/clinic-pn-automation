import csv
import re
from pathlib import Path

def main():
    data_dir = Path(__file__).parent.parent / "data"
    deeplink_map_path = data_dir / "deeplink_map.csv"
    
    unique_campaign_values = set()
    cohort_names = []
    
    with open(deeplink_map_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cohort_name = row.get("Cohort Name", "").strip()
            android_url = row.get("android_base_url", "").strip()
            ios_url = row.get("ios_base_url", "").strip()
            
            if cohort_name:
                cohort_names.append(cohort_name)
            
            for url in [android_url, ios_url]:
                if not url or "{priority}" not in url:
                    continue
                
                parts = url.split("{priority}_")
                if len(parts) > 1:
                    value = parts[1].split("?")[0]
                    unique_campaign_values.add(value)
    
    sorted_cohorts = sorted(cohort_names)
    sorted_campaigns = sorted(unique_campaign_values)
    
    print("=== Summary Folder List (Cohort Names) ===")
    for c in sorted_cohorts:
        print(c)
    
    print(f"\n=== Unique utm_campaign Values (after priority) ===")
    for v in sorted_campaigns:
        print(v)
    
    print(f"\n=== Summary ===")
    print(f"Total cohorts: {len(sorted_cohorts)}")
    print(f"Total unique campaign values: {len(sorted_campaigns)}")

if __name__ == "__main__":
    main()