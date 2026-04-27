import os
import zipfile
import shutil
import logging
import subprocess
import sys
import boto3
from pathlib import Path
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATASET = "kanakbaghel/hospital-management-dataset"
BUCKET = os.environ.get("S3_BUCKET", "uni-kcebuen")
#S3_PREFIX = f"kaggle-ds/{DATASET.split('/')[-1]}/{date.today().isoformat()}"
S3_PREFIX = f"kaggle-ds/{DATASET.split('/')[-1]}/{date.today().strftime('%Y%m%d')}"
LOCAL_DIR = Path("data")
ZIP_FILE = Path(f"{DATASET.split('/')[-1]}.zip")


def download_dataset():
    logger.info(f"Downloading dataset: {DATASET}")
    result = subprocess.run(
        ["kaggle", "datasets", "download", "-d", DATASET],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"Download failed: {result.stderr}")
        sys.exit(1)
    if not ZIP_FILE.exists():
        logger.error(f"Expected zip file not found: {ZIP_FILE}")
        sys.exit(1)
    logger.info(f"Downloaded: {ZIP_FILE} ({ZIP_FILE.stat().st_size / 1024:.1f} KB)")


def extract_dataset():
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(ZIP_FILE, "r") as zf:
        zf.extractall(LOCAL_DIR)
    files = list(LOCAL_DIR.rglob("*.*"))
    logger.info(f"Extracted {len(files)} files to {LOCAL_DIR}/")


def upload_to_s3():
    s3 = boto3.client("s3")
    files = [f for f in LOCAL_DIR.rglob("*") if f.is_file()]
    if not files:
        logger.error("No files to upload!")
        sys.exit(1)
    logger.info(f"Uploading {len(files)} files to s3://{BUCKET}/{S3_PREFIX}/")
    for f in files:
        relative = f.relative_to(LOCAL_DIR)
        key = f"{S3_PREFIX}/{relative}"
        s3.upload_file(str(f), BUCKET, key)
        logger.info(f"  ✓ {key} ({f.stat().st_size / 1024:.1f} KB)")


def cleanup():
    shutil.rmtree(LOCAL_DIR, ignore_errors=True)
    ZIP_FILE.unlink(missing_ok=True)
    logger.info("Cleaned up temp files.")


if __name__ == "__main__":
    try:
        download_dataset()
        extract_dataset()
        upload_to_s3()
        logger.info("Ingest complete!")
    except Exception as e:
        logger.error(f"Ingest failed: {e}")
        sys.exit(1)
    finally:
        cleanup()