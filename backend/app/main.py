from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
import os
import traceback
import time

from app.routes.power_plants import router as power_plants_router
from app.utils.logger import logger, log_audit

# For demonstration purposes - in a real app, this would be a more robust authentication system
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def get_user_id(api_key: str = Depends(api_key_header)):
    # Simple user identification - in production, use proper authentication
    if not api_key:
        return "anonymous"
    # In a real application, you would validate the API key and return the actual user ID
    return api_key[:8]  # Using first 8 chars of API key as user ID for demonstration

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

# Add global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    error_msg = f"Unhandled error: {str(exc)}"
    logger.error(f"Request to {request.url} failed: {error_msg}")
    logger.error(f"Exception details: {traceback.format_exc()}")
    
    # Audit log for errors
    user_id = getattr(request.state, "user_id", "system")
    log_audit(
        user_id=user_id,
        action="API_ERROR",
        resource=str(request.url),
        status="FAILURE",
        details=error_msg
    )
    
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal server error occurred."},
    )

# Add middleware for request logging
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    path = request.url.path
    method = request.method
    
    # Get user ID if available
    user_id = "anonymous"
    if "X-API-Key" in request.headers:
        user_id = request.headers["X-API-Key"][:8]  # Simplified for demo
    
    # Store user ID in request state for use in exception handler
    request.state.user_id = user_id
    
    logger.info(f"Request started: {method} {path}")
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        status_code = response.status_code
        
        logger.info(f"Request completed: {method} {path} - Status: {status_code} - Duration: {process_time:.4f}s")
        
        # Audit log for all API requests
        log_audit(
            user_id=user_id,
            action=method,
            resource=path,
            status="SUCCESS" if status_code < 400 else "FAILURE",
            details=f"Status: {status_code}, Duration: {process_time:.4f}s"
        )
        
        return response
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"Request failed: {method} {path} - Duration: {process_time:.4f}s - Error: {str(e)}")
        
        # Audit log is handled in the exception handler
        raise

@app.get("/")
async def read_root(user_id: str = Depends(get_user_id)):
    logger.info("Root endpoint accessed")
    log_audit(user_id, "READ", "root_endpoint", "SUCCESS")
    return {"message": "Welcome to the Power Plant API"}

if __name__ == "__main__":
    logger.info("Starting Power Plant API server")
    log_audit("system", "SYSTEM", "api_server", "STARTED")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 