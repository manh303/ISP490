import asyncio
import asyncpg

async def check_models():
    conn = await asyncpg.connect('postgresql://postgres:postgres@localhost:5432/ecommerce_dwh')
    rows = await conn.fetch('''
        SELECT model_name, model_type, model_version, status, created_at
        FROM ml.dim_ml_model
        ORDER BY created_at DESC
    ''')
    await conn.close()
    
    if rows:
        print("Models in registry:")
        print("-" * 80)
        for r in rows:
            print(f"{r['model_name']:30} | {r['model_type']:15} | {r['model_version']:10} | {r['status']}")
    else:
        print("No models found in registry")

asyncio.run(check_models())
