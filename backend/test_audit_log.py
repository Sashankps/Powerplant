#!/usr/bin/env python3
"""
Test script to verify audit logging functionality.
"""
import time
import random
from app.utils.logger import logger, log_audit

def main():
    """
    Run a simple test to verify audit logging functionality.
    """
    logger.info("Starting audit log test")
    print("Testing audit logging functionality...")
    
    # Generate some test audit log entries
    users = ["admin", "user1", "user2", "anonymous"]
    actions = ["READ", "CREATE", "UPDATE", "DELETE", "LOGIN", "LOGOUT"]
    resources = ["user_profile", "dashboard", "settings", "report", "api_endpoint"]
    statuses = ["SUCCESS", "FAILURE", "WARNING", "PENDING"]
    
    # Log some random audit events
    for i in range(10):
        user = random.choice(users)
        action = random.choice(actions)
        resource = random.choice(resources)
        status = random.choice(statuses)
        details = f"Test audit log entry #{i+1}"
        
        log_audit(user, action, resource, status, details)
        print(f"Created audit log: USER:{user} ACTION:{action} RESOURCE:{resource} STATUS:{status}")
        time.sleep(0.5)
    
    logger.info("Audit log test completed")
    print("\nTest completed. Check logs/audit.log for the audit entries.")
    print("Also check logs/app.log for application logs and logs/error.log for any errors.")

if __name__ == "__main__":
    main() 