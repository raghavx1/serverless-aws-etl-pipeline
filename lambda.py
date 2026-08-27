import os
import urllib.parse
import boto3
import pandas as pd
import pg8000.native

s3_client = boto3.client('s3')

# Grabbing DB credentials from env vars so they aren't hardcoded in the script
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']

def get_db_connection():
    # I went with pg8000 here instead of psycopg2 because it comes pre-packaged 
    # with the AWS Data Wrangler (Pandas) Lambda layer, which saves a lot of headache!
    return pg8000.native.Connection(
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        database=DB_NAME,
        port=5432
    )

def lambda_handler(event, context):
    # Extract the bucket name and file key from the S3 trigger event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # Using the filename as the batch_id for tracking and idempotency
    batch_id = key 

    print(f"Waking up! Processing s3://{bucket}/{key} | Batch ID: {batch_id}")

    # Read the CSV straight from S3 into a pandas dataframe
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])

    # Standardize data types before we try to validate anything
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

    # Data Quality Rules: No missing IDs, amount must be > 0, date must be valid
    valid_mask = (
        df['transaction_id'].notna() &
        df['customer_id'].notna() &
        df['product_id'].notna() &
        (df['amount'] > 0) &
        df['order_date'].notna()
    )

    # Split the data into the good stuff and the garbage
    good_data = df[valid_mask].copy()
    bad_data = df[~valid_mask].copy()

    conn = get_db_connection()

    try:
        # 1. Quarantine the bad data so we can investigate it later without crashing the pipeline
        if not bad_data.empty:
            for _, row in bad_data.iterrows():
                conn.run(
                    "INSERT INTO quarantine_sales (raw_record, error_reason, batch_id) VALUES (:raw_record, :error_reason, :batch_id)",
                    raw_record=row.to_json(),
                    error_reason="Failed validation (nulls or invalid types)",
                    batch_id=batch_id
                )
            print(f"Sent {len(bad_data)} corrupted records to the quarantine table.")

        if good_data.empty:
            print("No valid data found in this batch. Exiting early.")
            return {"status": "success", "message": "Only bad data found, quarantined."}

        # 2. Idempotency Check: Delete existing records for this batch ID
        # This ensures that if the Lambda retries, we don't end up with duplicate rows in our DB
        conn.run("DELETE FROM fact_transactions WHERE batch_id = :batch_id", batch_id=batch_id)

        # 3. Load Customers (Dimension) with Upsert logic
        for _, row in good_data[['customer_id', 'customer_name', 'email']].drop_duplicates().iterrows():
            conn.run("""
                INSERT INTO dim_customer (customer_id, customer_name, email) 
                VALUES (:cid, :cname, :email)
                ON CONFLICT (customer_id) DO UPDATE SET 
                customer_name = EXCLUDED.customer_name, email = EXCLUDED.email
            """, cid=row['customer_id'], cname=row['customer_name'], email=row['email'])

        # 4. Load Products (Dimension) with Upsert logic
        for _, row in good_data[['product_id', 'product_name', 'category']].drop_duplicates().iterrows():
            conn.run("""
                INSERT INTO dim_product (product_id, product_name, category) 
                VALUES (:pid, :pname, :cat)
                ON CONFLICT (product_id) DO UPDATE SET 
                product_name = EXCLUDED.product_name, category = EXCLUDED.category
            """, pid=row['product_id'], pname=row['product_name'], cat=row['category'])

        # 5. Load the Fact Table
        for _, row in good_data.iterrows():
            conn.run("""
                INSERT INTO fact_transactions (transaction_id, customer_id, product_id, order_date, amount, batch_id)
                VALUES (:tid, :cid, :pid, :odate, :amt, :bid)
                ON CONFLICT (transaction_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id, product_id = EXCLUDED.product_id, 
                order_date = EXCLUDED.order_date, amount = EXCLUDED.amount, batch_id = EXCLUDED.batch_id
            """, 
            tid=row['transaction_id'], cid=row['customer_id'], pid=row['product_id'], 
            odate=row['order_date'], amt=row['amount'], bid=batch_id)

        print(f"Success! Loaded {len(good_data)} clean records into the warehouse.")

    except Exception as e:
        print(f"Uh oh, database operation failed: {e}")
        raise e
    finally:
        # Always clean up the connection!
        conn.close()

    return {"status": "success", "processed": len(good_data), "quarantined": len(bad_data)}
