import logging
import os
from logging.handlers import RotatingFileHandler
import sys
from pathlib import Path

# Create logs directory if it doesn't exist
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Configure logger
def get_logger(name: str, log_level: int = logging.INFO) -> logging.Logger:
    """
    Configure and return a logger instance
    
    Args:
        name: Name of the logger (usually the module name)
        log_level: Logging level (default: INFO)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid adding handlers if they already exist
    if logger.hasHandlers():
        return logger
        
    logger.setLevel(log_level)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    
    # Create console handler for logging to stderr
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)
    
    # Create file handler for error logs
    error_file_handler = RotatingFileHandler(
        logs_dir / "error.log",
        maxBytes=10485760,  # 10MB
        backupCount=5,  # Keep 5 backup logs
    )
    error_file_handler.setFormatter(file_formatter)
    error_file_handler.setLevel(logging.ERROR)
    logger.addHandler(error_file_handler)
    
    # Create file handler for all logs
    all_file_handler = RotatingFileHandler(
        logs_dir / "app.log",
        maxBytes=10485760,  # 10MB
        backupCount=5,  # Keep 5 backup logs
    )
    all_file_handler.setFormatter(file_formatter)
    all_file_handler.setLevel(log_level)
    logger.addHandler(all_file_handler)
    
    # Create file handler for audit logs
    audit_file_handler = RotatingFileHandler(
        logs_dir / "audit.log",
        maxBytes=10485760,  # 10MB
        backupCount=5,  # Keep 5 backup logs
    )
    audit_file_handler.setFormatter(file_formatter)
    audit_file_handler.setLevel(log_level)
    # We don't add this handler to the logger as we'll use it separately
    
    return logger, audit_file_handler

# Create default logger instance
logger, audit_handler = get_logger("power_plant_api")

# Create specialized audit logger
audit_logger = logging.getLogger("audit")
if not audit_logger.handlers:
    audit_logger.setLevel(logging.INFO)
    audit_logger.addHandler(audit_handler)
    # Don't propagate to root logger to avoid duplicate logs
    audit_logger.propagate = False

def log_audit(user_id: str, action: str, resource: str, status: str, details: str = None):
    """
    Log an audit event
    
    Args:
        user_id: ID of the user performing the action
        action: Type of action (e.g., "READ", "CREATE", "UPDATE", "DELETE")
        resource: The resource being accessed
        status: Status of the action (e.g., "SUCCESS", "FAILURE")
        details: Additional details about the action
    """
    message = f"USER:{user_id} ACTION:{action} RESOURCE:{resource} STATUS:{status}"
    if details:
        message += f" DETAILS:{details}"
    audit_logger.info(message) 