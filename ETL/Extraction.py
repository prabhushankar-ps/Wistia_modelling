import os
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3
import json

# Initialize S3 client
s3 = boto3.client("s3")

BUCKET_NAME = "wistia-modelling"
S3_PREFIX = "Bronze_layer"

load_dotenv()
API_TOKEN = os.getenv("WISTIA_API_TOKEN")

# ==============================
# Configuration
# ==============================

# NEVER hardcode tokens in production
API_TOKEN = os.getenv("WISTIA_API_TOKEN")

if not API_TOKEN:
    raise ValueError("WISTIA_API_TOKEN environment variable not set")

BASE_URL = "https://api.wistia.com/v1/stats/medias"

# Media IDs provided in requirement doc
MEDIA_IDS = ["gskhw4w4lm", "v08dlrgr7v"]

# Output directory (simulate raw bronze layer)
OUTPUT_DIR = "raw_data"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==============================
# Helper: Get Yesterday Date
# ==============================

def get_yesterday_date():
    yesterday = datetime.utcnow() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


# ==============================
# API Call Function
# ==============================

def fetch_media_stats(media_id, date_str):
    """
    Fetch stats for a single media ID for a specific date.
    """

    url = f"{BASE_URL}/{media_id}.json"
    
    params = {
        "start_date": date_str,
        "end_date": date_str
    }

    headers = {
        "Authorization": f"Bearer {API_TOKEN}"
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 200:
            print(f"✅ Success: Pulled data for {media_id} ({date_str})")
            return response.json()

        elif response.status_code == 401:
            print("❌ Unauthorized – Check API token")
            return None

        elif response.status_code == 404:
            print(f"❌ Media ID {media_id} not found")
            return None

        else:
            print(f"⚠️ Error {response.status_code}: {response.text}")
            return None

    except Exception as e:
        print(f"🔥 Exception occurred for {media_id}: {str(e)}")
        return None


# ==============================
# Save Raw JSON
# ==============================

def save_raw_data_to_s3(media_id, date_str, data):
    """
    Upload raw JSON to S3 Bronze layer
    """

    file_key = f"{S3_PREFIX}/media_{media_id}_{date_str}.json"

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        print(f"☁️ Uploaded to s3://{BUCKET_NAME}/{file_key}")

    except Exception as e:
        print(f"🔥 Failed to upload to S3: {str(e)}")


# ==============================
# Main Execution
# ==============================

def main():
    date_str = get_yesterday_date()

    print(f"\n🚀 Starting Wistia Daily Extraction for {date_str}\n")

    for media_id in MEDIA_IDS:
        data = fetch_media_stats(media_id, date_str)

        if data:
            save_raw_data_to_s3(media_id, date_str, data)

    print("\n🎯 Daily extraction completed\n")


if __name__ == "__main__":
    main()