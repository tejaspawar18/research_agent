import os
import boto3
from core.utils import ensure_dir, today_path
from core.logger import get_logger
from dotenv import load_dotenv

load_dotenv()
logger = get_logger("file_manager")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

BUCKET = os.getenv("S3_BUCKET")

def save_local_pdf(file_path, pdf_bytes=None, filename=None):
    """Save PDF locally in /data/pdfs/YYYY/MM/DD."""
    base_dir = ensure_dir(f"data/pdfs/{today_path()}")
    filename = filename or os.path.basename(file_path)

    final_path = os.path.join(base_dir, filename)

    if pdf_bytes:
        with open(final_path, "wb") as f:
            f.write(pdf_bytes)
    else:
        os.rename(file_path, final_path)

    logger.info(f"Saved locally → {final_path}")
    return final_path

def upload_s3(local_path, key_prefix="papers"):
    if not BUCKET:
        logger.warning("No S3 bucket configured. Skipping upload.")
        return None

    key = f"{key_prefix}/{today_path()}/{os.path.basename(local_path)}"
    logger.info(f"S3 Upload → {key}")

    s3_client.upload_file(local_path, BUCKET, key)
    s3_url = f"s3://{BUCKET}/{key}"
    logger.info(f"Uploaded to S3 → {s3_url}")
    return s3_url
