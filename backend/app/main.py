from fastapi import FastAPI
from .api.v1 import analytics
app = FastAPI(title="Ecommerce DSS API")
app.include_router(analytics.router, prefix="/v1/analytics")
@app.get("/health")
def health(): return {"status":"ok"}