import os
from typing import Optional, List, Dict, Any
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from jwt.exceptions import PyJWTError

# Clerk JWT verification settings
CLERK_PEM_PUBLIC_KEY = os.getenv("CLERK_PEM_PUBLIC_KEY", "")
CLERK_JWKS_URL = os.getenv("CLERK_JWKS_URL", "")

if not CLERK_PEM_PUBLIC_KEY and not CLERK_JWKS_URL:
    raise ValueError("Either CLERK_PEM_PUBLIC_KEY or CLERK_JWKS_URL must be set")

# Security scheme for JWT authentication
security = HTTPBearer()

def verify_jwt_token(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """
    Verify JWT token from Clerk.
    Returns the decoded token if valid.
    """
    token = credentials.credentials
    try:
        # Use public key if available
        if CLERK_PEM_PUBLIC_KEY:
            payload = jwt.decode(
                token,
                CLERK_PEM_PUBLIC_KEY,
                algorithms=["RS256"],
                audience="YOUR_AUDIENCE",  # Set this to your API audience
                options={"verify_aud": False},  # Set to True in production with correct audience
            )
        # Otherwise use JWKS URL
        else:
            # Note: For production, you should implement JWKS support
            # This is simplified for example purposes
            raise NotImplementedError("JWKS URL support not implemented in this example")
        
        return payload
    except PyJWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid authentication credentials: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )

# Dependency to get the current user from token
def get_current_user(token_data: Dict[str, Any] = Depends(verify_jwt_token)) -> Dict[str, Any]:
    """
    Extract user information from the JWT token
    """
    if not token_data.get("sub"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user information in token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # You can enhance this to fetch more user data if needed
    user_data = {
        "id": token_data.get("sub"),
        "email": token_data.get("email", ""),
        "name": token_data.get("name", ""),
    }
    
    return user_data 