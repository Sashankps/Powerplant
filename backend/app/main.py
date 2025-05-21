from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from app.routes.power_plants import router as power_plants_router

app = FastAPI(
    title="Power Plant API",
    description="API for visualizing power plant net generation data from EPA's eGRID dataset",
    version="1.0.0",
)

# Get frontend URL from environment or use default values
frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5173")
# Add additional URLs if needed
allowed_origins = [
    frontend_url, 
    "http://localhost:3000",  # Docker frontend URL
    "http://localhost:80",    # Nginx port
    "http://localhost"        # Base localhost
]

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Allow multiple origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(power_plants_router)

@app.get("/")
async def read_root():
    return {"message": "Welcome to the Power Plant API"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 