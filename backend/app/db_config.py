import os


# Load .env file FIRST before getting database URL
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _ensure_sslmode(url: str) -> str:
    """Append sslmode=require if missing to keep Render TLS happy."""
    if "sslmode=" in url:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}sslmode=require"


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
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    
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
