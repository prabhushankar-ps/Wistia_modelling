import boto3
import pandas as pd
import json
from io import BytesIO
from datetime import datetime, timedelta

yesterday_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

# ==========================
# Configuration
# ==========================

BUCKET_NAME = "wistia-modelling"
BRONZE_PREFIX = "Bronze_layer/"
SILVER_PREFIX = "silver_layer/"

s3 = boto3.client("s3")


# ==========================
# Step 1: Read Bronze Files
# ==========================

def get_bronze_files():
    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=BRONZE_PREFIX
    )

    files = []

    for obj in response.get("Contents", []):
        key = obj["Key"]

        if key.endswith(".json") and yesterday_str in key:
            files.append(key)

    return files


def read_json_from_s3(key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    content = response["Body"].read()
    return json.loads(content)


# ==========================
# Step 2: Transform to Structured Format
# ==========================

def transform_to_dataframe(raw_json):
    """
    Adjust this logic based on actual Wistia JSON structure.
    """

    # Example assumptions — adjust based on actual JSON fields
    media_id = raw_json.get("media_id")
    title = raw_json.get("name")
    created_at = raw_json.get("created_at")

    stats = raw_json.get("stats", {})

    row = {
        "media_id": media_id,
        "title": title,
        "created_at": created_at,
        "play_count": stats.get("plays"),
        "play_rate": stats.get("play_rate"),
        "total_watch_time": stats.get("total_watch_time"),
        "watched_percent": stats.get("average_percent_watched")
    }

    df = pd.DataFrame([row])
    return df


# ==========================
# Step 3: Write Parquet to S3
# ==========================

def upload_parquet_to_s3(df, file_name):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{SILVER_PREFIX}{file_name}",
        Body=buffer.getvalue()
    )


# ==========================
# Main Execution
# ==========================

def main():
    bronze_files = get_bronze_files()

    print(f"Found {len(bronze_files)} bronze files")

    for key in bronze_files:
        print(f"Processing {key}")

        raw_json = read_json_from_s3(key)
        df = transform_to_dataframe(raw_json)

        # Create silver file name
        base_name = key.split("/")[-1].replace(".json", ".parquet")

        upload_parquet_to_s3(df, base_name)

        print(f"Uploaded silver file: {base_name}")


if __name__ == "__main__":
    main()