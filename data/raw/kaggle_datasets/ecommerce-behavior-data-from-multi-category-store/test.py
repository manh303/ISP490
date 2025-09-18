count = 0
with open("2019-Nov.csv", "r", encoding="utf-8") as f:
    for _ in f:
        count += 1

print(f"Tổng số dòng: {count}")