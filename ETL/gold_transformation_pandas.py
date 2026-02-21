import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

BUCKET_NAME = "wistia-modelling"
SILVER_PREFIX = "silver_layer/"
GOLD_PREFIX = "gold_layer/"

s3 = boto3.client("s3")

# Yesterday logic (must match ingestion)
yesterday_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")


# ==========================
# Helper: Get Yesterday Silver Files
# ==========================

def get_silver_files():
    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=SILVER_PREFIX
    )

    files = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".parquet") and yesterday_str in key:
            files.append(key)

    return files


def read_parquet_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def upload_parquet(df, path):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=path, Body=buffer.getvalue())


# ==========================
# Gold Transformation
# ==========================

def main():
    silver_files = get_silver_files()

    if not silver_files:
        print("No silver files found for yesterday")
        return

    dfs = [read_parquet_from_s3(key) for key in silver_files]
    df = pd.concat(dfs, ignore_index=True)

    # -----------------------
    # DIM_MEDIA
    # -----------------------
    dim_media = df[["media_id", "title", "created_at"]].drop_duplicates()

    upload_parquet(
        dim_media,
        f"{GOLD_PREFIX}dim_media_{yesterday_str}.parquet"
    )

    # -----------------------
    # FACT_MEDIA_ENGAGEMENT
    # -----------------------
    fact = df.copy()
    fact["date"] = yesterday_str

    fact = fact[[
        "media_id",
        "date",
        "play_count",
        "play_rate",
        "total_watch_time",
        "watched_percent"
    ]]

    upload_parquet(
        fact,
        f"{GOLD_PREFIX}fact_media_engagement_{yesterday_str}.parquet"
    )

    print("Gold layer written successfully.")


if __name__ == "__main__":
    main()