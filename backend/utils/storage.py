import os
import tempfile
from pathlib import Path

def get_upload_base_dir():
    
    custom_dir = os.getenv("LOCAL_UPLOAD_DIR")

    if custom_dir:
        base = Path(custom_dir)
    else:
        base = Path(tempfile.gettempdir()) / "uploads"

    base.mkdir(parents=True, exist_ok=True)
    return base
