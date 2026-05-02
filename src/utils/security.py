import logging
from fastapi import HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from datetime import datetime

# Konfigurasi Role-Based Access Control (RBAC)
API_KEYS = {
    "admin-secret-key-123": "admin",
    "viewer-secret-key-456": "viewer"
}

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def get_current_role(api_key: str = Security(api_key_header)):
    if not api_key:
        raise HTTPException(status_code=401, detail="X-API-Key header is missing. Access Denied.")
    role = API_KEYS.get(api_key)
    if not role:
        raise HTTPException(status_code=403, detail="Invalid API Key. Access Denied.")
    return role

def require_admin(role: str = Depends(get_current_role)):
    if role != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required for this action")
    return role

def audit_log(action: str, resource: str, user: str):
    """
    Simulasi Tamper-Proof Audit Logging
    Semua aksi penting akan dicatat secara permanen di file log.
    """
    timestamp = datetime.utcnow().isoformat()
    log_entry = f"[{timestamp}] AUDIT_EVENT | ACTION: {action} | RESOURCE: {resource} | BY: {user}\n"
    
    # Print ke console
    print(log_entry.strip())
    
    # Tulis permanen ke file (Append-Only)
    try:
        with open("security_audit.log", "a") as f:
            f.write(log_entry)
    except Exception as e:
        print(f"Failed to write audit log: {e}")
