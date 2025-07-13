import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
import time
import pytz
import os
import uuid
import json

# ----- CONFIG ----
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
S3_BUCKET = "dev-dolphins-bucket"
S3_PREFIX = "transaction_chunks/"

PG_USER = 'postgres'
PG_PASS = 'admin'
PG_DB = 'postgres'
PG_HOST = 'localhost'
PG_PORT = 5432
customer_importance_df = pd.read_csv('CustomerImportance.csv')

PATTERNS = ["UPGRADE", "CHILD", "DEI-NEEDED"]
IST = pytz.timezone("Asia/Kolkata")
Y_START_TIME = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')

LOCAL_CHUNK_FOLDER = "/tmp/s3chunks/"
os.makedirs(LOCAL_CHUNK_FOLDER, exist_ok=True)

# ------------- POSTGRES BOILERPLATE ----------------
def get_pg_conn():
    return psycopg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        host=PG_HOST,
        port=PG_PORT
    )

def init_state_tables():
    conn = get_pg_conn()
    cur = conn.cursor()
    # Storing stats to support fast, incremental calculation
    cur.execute("""
    CREATE TABLE IF NOT EXISTS y_processed_chunks (
        chunk_key TEXT PRIMARY KEY
    );
    CREATE TABLE IF NOT EXISTS y_merchant_txn_stats (
        merchant TEXT,
        customer TEXT,
        gender TEXT,
        total_txns INTEGER,
        total_amt FLOAT,
        PRIMARY KEY (merchant, customer)
    );
    CREATE TABLE IF NOT EXISTS y_merchant_agg (
        merchant TEXT PRIMARY KEY,
        total_merch_txns INTEGER
    );
    CREATE TABLE IF NOT EXISTS y_merchant_genders (
        merchant TEXT,
        gender TEXT,
        n INTEGER,
        PRIMARY KEY (merchant, gender)
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

# Process and update summary tables
def update_temp_state(df):
    import numpy as np

    conn = get_pg_conn()
    cur = conn.cursor()

    # Clean the DataFrame up front to prevent type issues
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
    if 'merchant' in df.columns:
        df['merchant'] = df['merchant'].astype(str)
    if 'customer' in df.columns:
        df['customer'] = df['customer'].astype(str)
    if 'gender' in df.columns:
        df['gender'] = df['gender'].astype(str)

    # Update gender count per merchant
    for (merchant, gender), gdf in df.groupby(['merchant','gender']):
        count = int(len(gdf))
        cur.execute("""
            INSERT INTO y_merchant_genders (merchant, gender, n)
            VALUES (%s, %s, %s)
            ON CONFLICT (merchant, gender)
            DO UPDATE SET n = y_merchant_genders.n + %s
        """, (merchant, gender, count, count))

    # Update per-customer+merchant stats
    rows = []
    for (merchant, customer, gender), gdf in df.groupby(['merchant','customer','gender']):
        total_txns = int(len(gdf))
        amt_sum = float(gdf['amount'].sum())
        # Defensive: never send types, only actual numeric values
        if not isinstance(total_txns, int) or not isinstance(amt_sum, float):
            total_txns = int(total_txns)
            amt_sum = float(amt_sum)
        rows.append((merchant, customer, gender, total_txns, amt_sum))

    execute_batch(cur, """
        INSERT INTO y_merchant_txn_stats (merchant, customer, gender,
                                   total_txns, total_amt)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (merchant, customer)
        DO UPDATE SET
           total_txns = y_merchant_txn_stats.total_txns + EXCLUDED.total_txns,
           total_amt = y_merchant_txn_stats.total_amt + EXCLUDED.total_amt
    """, rows)

    # Update merchant total transactions
    for merchant, mdf in df.groupby('merchant'):
        cnt = int(len(mdf))
        cur.execute("""
            INSERT INTO y_merchant_agg (merchant, total_merch_txns)
            VALUES (%s, %s)
            ON CONFLICT (merchant) DO UPDATE SET
                total_merch_txns = y_merchant_agg.total_merch_txns + %s
        """, (merchant, cnt, cnt))

    conn.commit()
    cur.close()
    conn.close()


# Extract merchant stats for pattern checks
def fetch_merchant_stats():
    conn = get_pg_conn()
    cur = conn.cursor()
    # Pattern 1
    cur.execute("""
        SELECT merchant, total_merch_txns FROM y_merchant_agg WHERE total_merch_txns >= 50000
    """)
    big_merchants = {r[0]: r[1] for r in cur.fetchall()}

    # per merchant: customer stats for P1/P2
    cur.execute("""
        SELECT merchant, customer, gender, total_txns, total_amt FROM y_merchant_txn_stats
    """)
    cust_rows = cur.fetchall()

    # per merchant, gender stats for P3
    cur.execute("""
        SELECT merchant, gender, n FROM y_merchant_genders
    """)
    gender_rows = cur.fetchall()
    cur.close()
    conn.close()
    return big_merchants, cust_rows, gender_rows

# Mark S3 chunk as processed
def mark_chunk_processed(key):
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO y_processed_chunks (chunk_key) VALUES (%s) ON CONFLICT DO NOTHING", (key,))
    conn.commit()
    cur.close()
    conn.close()

def already_processed_chunk(key):
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM y_processed_chunks WHERE chunk_key=%s", (key,))
    yes = cur.fetchone() is not None
    cur.close()
    conn.close()
    return yes

# ------------- LOGIC ------------------

def detect_patterns(big_merchants, cust_rows, gender_rows):
    detections = []

    now_ist = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')

    # --- PatId1: UPGRADE ---
    for (merchant, merch_txns) in big_merchants.items():
        cust_stats = [
            r for r in cust_rows if r[0] == merchant
        ]
        if not cust_stats: continue
        txn_counts = [r[3] for r in cust_stats]
        weights = [r[4]/r[3] if r[3]>0 else 0 for r in cust_stats]
        if len(txn_counts) < 10: continue  # need enough data points

        top_idx = int(0.9 * len(txn_counts))
        bot_idx = int(0.1 * len(txn_counts))
        txn_sorted = sorted(zip(txn_counts, weights, cust_stats),
                            key=lambda x: x[0])
        # bottom 10% of weights
        bottom_weights = [w for _, w, _ in txn_sorted[:bot_idx+1]]
        avg_bottom_weight = sum(bottom_weights) / len(bottom_weights) if bottom_weights else 0
        # top 10% customers
        top_custs = txn_sorted[top_idx:]
        for cnt, w, row in top_custs:
            if w <= avg_bottom_weight:
                # Detected: merchant, customer
                detection = {
                    "YStartTime": Y_START_TIME,
                    "detectionTime": now_ist,
                    "patternId": "PatId1",
                    "ActionType": "UPGRADE",
                    "customerName": row[1],  # customer
                    "MerchantId": merchant,
                }
                detections.append(detection)

    # --- PatId2: CHILD ---
    for row in cust_rows:
        merchant, customer, gender, total_txns, total_amt = row
        if total_txns >= 80:
            avg_val = total_amt / total_txns if total_txns else 0
            if avg_val < 23:
                detection = {
                    "YStartTime": Y_START_TIME,
                    "detectionTime": now_ist,
                    "patternId": "PatId2",
                    "ActionType": "CHILD",
                    "customerName": customer,
                    "MerchantId": merchant,
                }
                detections.append(detection)

    # --- PatId3: DEI-NEEDED ---
    from collections import defaultdict
    merch_gender = defaultdict(lambda: {'M': 0, 'F': 0})
    for merchant, gender, n in gender_rows:
        merch_gender[merchant][gender] = n
    for merchant, gdict in merch_gender.items():
        if gdict['F'] > 100 and gdict['F'] < gdict['M']:
            detection = {
                "YStartTime": Y_START_TIME,
                "detectionTime": now_ist,
                "patternId": "PatId3",
                "ActionType": "DEI-NEEDED",
                "customerName": "",
                "MerchantId": merchant,
            }
            detections.append(detection)

    return detections

def poll_and_process():
    s3 = boto3.client('s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY)

    detection_buffer = []
    print("Mechanism Y started at", Y_START_TIME, "(IST)")

    while True:
        # List objects in S3
        res = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        for obj in res.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.csv') or already_processed_chunk(key):
                continue
            # Download
            local_path = os.path.join(LOCAL_CHUNK_FOLDER, os.path.basename(key))
            s3.download_file(S3_BUCKET, key, local_path)
            # Load chunk
            df = pd.read_csv(local_path)
            # Clean up column names if needed
            df.columns = [c.strip().lower() for c in df.columns]

            # Update stats (temp state)
            update_temp_state(df)
            mark_chunk_processed(key)
            print(f"Processed {key}.")

            # Fetch state
            big_merchants, cust_rows, gender_rows = fetch_merchant_stats()
            detections = detect_patterns(big_merchants, cust_rows, gender_rows)
            detection_buffer.extend(detections)

            # Output every 50
            while len(detection_buffer) >= 50:
                out_rows = detection_buffer[:50]
                detection_buffer = detection_buffer[50:]
                fname = f"detections_{uuid.uuid4()}.jsonl"
                fpath = os.path.join(LOCAL_CHUNK_FOLDER, fname)
                with open(fpath, "w") as fout:
                    for det in out_rows:
                        fout.write(json.dumps(det) + "\n")
                # Upload to S3
                outkey = f"mechanism_Y_detections/{fname}"
                s3.upload_file(fpath, S3_BUCKET, outkey)
                print(f"Wrote detections batch: {outkey}")

            os.remove(local_path)
        # Poll every 5 seconds for new files
        time.sleep(5)

if __name__ == "__main__":
    init_state_tables()
    poll_and_process()