import os


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
        return _ensure_sslmode(env_url.strip())
    host = os.getenv("DB_HOST", "dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "ecommerce_dss_1")
    user = os.getenv("DB_USER", "dss_user")
    password = os.getenv("DB_PASSWORD", "6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G")

    url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
    return _ensure_sslmode(url)


# Export a shared constant for all modules.
DATABASE_URL = get_database_url()
