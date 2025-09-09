from fastapi import APIRouter
router = APIRouter()
@router.get("/kpi")
def kpi(): return {"orders": 0, "gmv": 0, "aov": 0}