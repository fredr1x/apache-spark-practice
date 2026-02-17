import csv
import time

start = time.perf_counter()

ages = []
filtered = []

with open("people.csv", newline="") as file:
    reader = csv.DictReader(file)
    for row in reader:
        if row["age"]:
            age = int(row["age"])
            ages.append(age)
            if age > 25:
                filtered.append(row)

avg_age = sum(ages) / len(ages)
print("avg age:", avg_age)

print("age > 25:")
for row in filtered:
    print(row)

end = time.perf_counter()
print(f"execution time:{end - start:6f} seconds")

# results
# avg age: 31.25
# age > 25:
# {'name': 'Alice', 'age': '34'}
# {'name': 'Charlie', 'age': '29'}
# {'name': '', 'age': '40'}
# execution time:0.001104 seconds