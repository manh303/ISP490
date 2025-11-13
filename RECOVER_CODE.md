# Khôi phục code bị mất sau khi pull

## Tình huống
Bạn đã pull code từ main về branch manh303 và bị mất code đã sửa.

## Giải pháp

### Option 1: Abort merge và giữ code hiện tại
```bash
# Hủy merge, giữ lại code của bạn
git merge --abort

# Xem code của bạn trước khi pull
git log --oneline -5
```

### Option 2: Khôi phục từ commit trước đó
```bash
# Xem lịch sử commit
git reflog

# Khôi phục về commit trước khi pull (4e49059)
git reset --hard 4e49059

# Hoặc merge lại nhưng ưu tiên code của bạn
git merge main -X ours
```

### Option 3: Cherry-pick commit cụ thể
```bash
# Lấy commit "add ml models" (4e49059)
git cherry-pick 4e49059
```

### Option 4: Stash và reapply
```bash
# Nếu code chưa commit
git stash
git pull origin main
git stash pop
```

## Khôi phục file cụ thể

### Khôi phục từ commit trước
```bash
# Khôi phục file từ commit 4e49059
git checkout 4e49059 -- data-pipeline/src/ml_models/
git checkout 4e49059 -- backend/app/api/v1/ml_insights.py
git checkout 4e49059 -- airflow/dags/tiki_lazada_elt_dag.py
```

### Khôi phục từ stash
```bash
# Xem stash list
git stash list

# Apply stash cụ thể
git stash apply stash@{0}
```

## Giải quyết conflicts hiện tại

Bạn có 4 files conflict:
```
deleted by us:   data-collection/crawlers/base_crawler.py
deleted by us:   data-collection/crawlers/lazada/runners/debug_lazada.py
deleted by us:   data-collection/crawlers/lazada/runners/output/fixed_lazada_multi_category_destops-computers_urls.csv
deleted by us:   data-collection/crawlers/lazada/runners/output/fixed_lazada_multi_category_headphones_urls.csv
```

### Giải quyết:
```bash
# Nếu muốn xóa files này (chấp nhận xóa)
git rm data-collection/crawlers/base_crawler.py
git rm data-collection/crawlers/lazada/runners/debug_lazada.py
git rm data-collection/crawlers/lazada/runners/output/fixed_lazada_multi_category_destops-computers_urls.csv
git rm data-collection/crawlers/lazada/runners/output/fixed_lazada_multi_category_headphones_urls.csv

# Commit merge
git commit -m "Merge main into manh303 - resolved conflicts"
```

## Khuyến nghị

### Cách tốt nhất: Reset về trước khi pull
```bash
# 1. Abort merge hiện tại
git merge --abort

# 2. Backup code hiện tại
git branch backup-manh303

# 3. Reset về commit trước pull
git reset --hard 4e49059

# 4. Merge lại với strategy ưu tiên code của bạn
git merge main -X ours -m "Merge main keeping our changes"

# 5. Nếu có conflict, giải quyết thủ công
```

### Nếu muốn giữ cả 2 phiên bản
```bash
# Xem diff giữa 2 versions
git diff HEAD main -- data-pipeline/src/ml_models/

# Merge thủ công từng file
git checkout --ours data-pipeline/src/ml_models/price_optimization.py
git checkout --theirs backend/app/main.py
```

## Phòng tránh lần sau

### 1. Luôn commit trước khi pull
```bash
git add .
git commit -m "WIP: save current work"
git pull origin main
```

### 2. Sử dụng rebase thay vì merge
```bash
git pull --rebase origin main
```

### 3. Backup thường xuyên
```bash
# Tạo branch backup
git branch backup-$(date +%Y%m%d)

# Hoặc push lên remote
git push origin manh303
```

### 4. Sử dụng stash
```bash
# Trước khi pull
git stash save "WIP: ML models and API"
git pull origin main
git stash pop
```

## Files quan trọng cần khôi phục

Dựa vào session trước, các files này cần được khôi phục:

1. **ML Models:**
   - `data-pipeline/src/ml_models/product_recommendation.py`
   - `data-pipeline/src/ml_models/price_optimization.py`
   - `data-pipeline/src/ml_models/demand_forecasting.py`
   - `data-pipeline/src/ml_models/sales_forecasting.py`

2. **API:**
   - `backend/app/api/v1/ml_insights.py`
   - `backend/app/main.py` (đã thêm ML router)

3. **DAG:**
   - `airflow/dags/tiki_lazada_elt_dag.py` (đã thêm ML tasks)

4. **Spark Jobs:**
   - `data-pipeline/src/spark_jobs/ods_transformation.py`
   - `data-pipeline/src/spark_jobs/dwh_build.py`
   - `data-pipeline/src/spark_jobs/datamart_build.py`

## Lệnh nhanh để khôi phục tất cả

```bash
# Abort merge
git merge --abort

# Reset về commit có ML models
git reset --hard 4e49059

# Nếu cần update từ main
git merge main -X ours

# Hoặc rebase
git rebase main
```
