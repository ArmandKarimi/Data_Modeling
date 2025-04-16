import boto3
import os

def upload_all_csvs(local_folder, bucket_name, s3_folder):
    s3 = boto3.client('s3')

    for filename in os.listdir(local_folder):
        if filename.endswith(".csv"):
            local_path = os.path.join(local_folder, filename)
            s3_key = f"{s3_folder}/{filename}"

            try:
                s3.upload_file(local_path, bucket_name, s3_key)
                print(f"✅ Uploaded: {filename} → s3://{bucket_name}/{s3_key}")
            except Exception as e:
                print(f"❌ Error uploading {filename}: {e}")

# Example usage
upload_all_csvs(
    local_folder="../data",           # Your actual path to local CSVs
    bucket_name="oecd-countries-data",    # Your S3 bucket name
    s3_folder="csv"                     # Optional folder inside bucket
)
