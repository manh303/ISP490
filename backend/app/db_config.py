import os


# Load .env file FIRST before getting database URL
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _ensure_sslmode(url: str) -> str:
    """Append sslmode=require if missing AND on production/Render environment."""
    # Only add sslmode=require for production environments (Render, external hosts)
    # Skip for localhost/local development
    is_localhost = "localhost" in url or "127.0.0.1" in url
    
    # If already has sslmode specified, return as-is
    if "sslmode=" in url:
        return url
    
    # Only add sslmode=require for production, not for localhost
    if not is_localhost:
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}sslmode=require"
    
    return url


def get_database_url() -> str:
    """
    Resolve database URL from env vars.
    Prefer DATABASE_URL, fallback to individual DB_* values.
    """
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        print(f"✅ Using DATABASE_URL from environment")
        return _ensure_sslmode(env_url.strip())
    
    # Fallback to individual components
    host = os.getenv("DB_HOST","localhost" )
    port = os.getenv("DB_PORT", "5433")
    name = os.getenv("DB_NAME","ecommerce_dss")
    user = os.getenv("DB_USER","dss_user")
    password = os.getenv("DB_PASSWORD","dss_password_123")
    
    if not all([host, name, user, password]):
        raise ValueError(
            "❌ Missing database configuration. "
            "Set either DATABASE_URL or all of: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD"
        )

    url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
    print(f"✅ Built DATABASE_URL from components")
    return _ensure_sslmode(url)


# Export a shared constant for all modules.
DATABASE_URL = get_database_url()
