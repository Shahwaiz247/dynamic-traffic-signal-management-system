import os
import sys
from pathlib import Path

# Add backend directory to Python sys.path
backend_path = Path(__file__).resolve().parent.parent / "backend"
if str(backend_path) not in sys.path:
    sys.path.insert(0, str(backend_path))

# Set serverless environment flags
os.environ["VERCEL"] = "1"
os.environ["FORCE_MOCK_HADOOP"] = "1"

# Import Flask app
from server import app

# Vercel serverless WSGI entry point
handler = app
