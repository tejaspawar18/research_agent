import hashlib
import os
from datetime import datetime
import uuid
import re

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

def sanitize_filename(name):
    """
    Sanitize a string to be safe for use as a filename.
    """
    # Remove invalid characters
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    # Replace whitespace with underscores
    name = name.replace(' ', '_')
    return name.strip()
