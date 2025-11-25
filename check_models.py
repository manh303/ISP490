import asyncpg
import asyncio
import pickle

# Đổi lại cho đúng connection string của bạn
DB_URL = "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1"

async def inspect_ml_tables():
    conn = await asyncpg.connect(DB_URL)

    print("\n=== LIST TABLES IN SCHEMA ml ===")
    tables = await conn.fetch("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'ml'
        ORDER BY table_name;
    """)
    for t in tables:
        print(" -", t["table_name"])

    print("\n=== CONTENT OF ml.models_storage ===")
    rows = await conn.fetch("""
        SELECT 
            model_id,
            model_name,
            model_type,
            version,
            status,
            created_at,
            octet_length(model_binary) AS size_bytes,
            model_binary                      -- 🔥 THÊM CỘT NÀY
        FROM ml.models_storage
        ORDER BY created_at DESC;
    """)

    for r in rows:
        print("\n-------------------------------")
        print(f"ID: {r['model_id']}")
        print(f"Name: {r['model_name']}")
        print(f"Type: {r['model_type']}")
        print(f"Version: {r['version']}")
        print(f"Status: {r['status']}")
        print(f"Created at: {r['created_at']}")
        print(f"Binary Size: {r['size_bytes']} bytes")

    print("\n=== CHECK MODEL OBJECT DETAILS ===")
    for r in rows:
        print(f"\n>>> MODEL_ID {r['model_id']} ({r['model_name']})")

        try:
            # 🔥 Unpickle binary
            obj = pickle.loads(r["model_binary"])

            # Nếu là dict (đúng pattern của bạn)
            if isinstance(obj, dict):
                print("Object type:", type(obj))
                for k, v in obj.items():
                    print(f" -> {k}: {type(v)}")
            else:
                # Pure model (không phải dict)
                print("Pure model object type:", type(obj))

        except Exception as e:
            print("❌ ERROR unpickling model:", e)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(inspect_ml_tables())
