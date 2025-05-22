import uvicorn
import os
from pathlib import Path

# Create logs directory if it doesn't exist
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

if __name__ == "__main__":
    print("Starting Power Plant API server...")
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True) 