import hashlib
import os
from datetime import datetime
import uuid

def generate_uuid():
    return str(uuid.uuid4())

def sha256_file(path):
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()

def today_path():
    now = datetime.utcnow()
    return f"{now.year}/{now.month:02}/{now.day:02}"

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)
    return path
