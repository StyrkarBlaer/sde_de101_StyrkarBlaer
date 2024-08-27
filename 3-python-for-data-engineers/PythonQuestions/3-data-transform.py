print(
    "################################################################################"
)
print("Use standard python libraries to do the transformations")
print(
    "################################################################################"
)

# Question: How do you read data from a CSV file at ./data/sample_data.csv into a list of dictionaries?
import csv

csv_file = './data/sample_data.csv'

with open(csv_file, mode='r', newline='') as file:
    csv_reader = csv.DictReader(csv_file)
    data = [row for row in csv_reader]
for row in data:
    print(row)

# Question: How do you remove duplicate rows based on customer ID?

ids = set()
uniq = []

for row in data:
    customer_id = row['customer_id']
    if customer_id not in ids:
        uniq.append(row)
        ids.add(customer_id)

# Question: How do you handle missing values by replacing them with 0?

for row in data:
    for key, value in row.items():
        if value is None or value == '':
            row[key] = 0

# Question: How do you remove outliers such as age > 100 or purchase amount > 1000?

filter = [
    row for row in data
    if row.get('age', 0) <= 100 or row.get('purchase_amount', 0) <= 1000
]

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male)?

for row in filter:
    if row['gender'] == 'female':
        row['gender'] = 0
    if row['gender'] == 'male':
        row['gender'] = 1

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns?

for row in filter:
    name_parts = row['customer_name'].split(maxsplit=1)

    row['first_name'] = name_parts[0]
    row['last_name'] = name_parts[1] if len(name_parts) > 1 else ''

    del row['customer_name']
    
# Question: How do you calculate the total purchase amount by Gender?

total_pruchases_by_gender = {}

for row in filter:
    gender = row['gender']
    purchases = row['purchases']

    if gender in total_pruchases_by_gender:
        total_pruchases_by_gender[gender] += purchases
    else:
        total_pruchases_by_gender[gender] = purchases

# Question: How do you calculate the average purchase amount by Age group?
# assume age_groups is the grouping we want
# hint: Why do we convert to float?
age_groups = {"18-30": [], "31-40": [], "41-50": [], "51-60": [], "61-70": []}

for row in filter:
    age = row['age']
    purchases = row['purchases']

    if age <= 30:
        age_groups["18-30"].append(purchases)
    elif age <= 40:
        age_groups["31-40"].append(purchases)
    elif age <= 50:
        age_groups["41-50"].append(purchases)
    elif age <= 60:
        age_groups["51-60"].append(purchases)
    elif age <= 70:
        age_groups["61-70"].append(purchases)

average_purchase_by_age_group = {}
for group, purchases in age_groups.items():
    if purchases:
        average_purchase_by_age_group[group] = sum(purchases / len(purchases))
    else:
        average_purchase_by_age_group[group] = 0

# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group?

print(f"Total purchase amount by Gender: {total_pruchases_by_gender}")
print(f"Average purchase amount by Age group: {average_purchase_by_age_group}")

print(
    "################################################################################"
)
print("Use DuckDB to do the transformations")
print(
    "################################################################################"
)

# Question: How do you connect to DuckDB and load data from a CSV file into a DuckDB table?
# Connect to DuckDB and load data
import duckdb

conn = duckdb.connect(database=":memory:", read_only=False)
conn.execute(
    "CREATE TABLE data (Customer_ID INTEGER, Customer_Name VARCHAR, Age INTEGER, Gender VARCHAR, Purchase_Amount FLOAT, Purchase_Date DATE)"
)

# Read data from CSV file into DuckDB table

conn.execute("""
    COPY data
    FROM './data/sample_data.csv' 
    WITH HEADER CSV
""")

# Question: How do you remove duplicate rows based on customer ID in DuckDB?

conn.execute(""""
    CREATE TABLE distinct_customers AS
    SELECT DISTINCT ON (customer_id) *
    FROM customers
""")

# Question: How do you handle missing values by replacing them with 0 in DuckDB?

conn.execute("""
    UPDATE customers
    SET age = COALESCE(age, 0),
    purchases = COALESCE(purchases, 0)
""")

# Question: How do you remove outliers (e.g., age > 100 or purchase amount > 1000) in DuckDB?

conn.execute("""
    DELETE FROM customers
    WHERE age > 100 OR purchase_amount > 1000
""")

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male) in DuckDB?

conn.execute("""
    UPDATE customers
    SET gender = CASE
        WHEN gender = 'Female' THEN 0
        ELSE 1
    END
""")

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns in DuckDB?

conn.execute("""
    UPDATE customers
    SET first_name = SPLIT_PART(customer_name, ' ', 1),
        last_name = SPLIT_PART(customer_name, ' ', 2)
""")

# Question: How do you calculate the total purchase amount by Gender in DuckDB?

total_pruchases_by_gender = conn.execute("""
    SELECT gender, SUM(purchases) AS total_purchases
    FROM customers
    GROUP BY gender
""").fetchall()

# Question: How do you calculate the average purchase amount by Age group in DuckDB?

average_purchase_by_age_group = conn.execute("""
    SELECT 
        CASE
            WHEN age BETWEEN 18 AND 30 THEN '18-30'
            WHEN age BETWEEN 31 AND 40 THEN '31-40'
            WHEN age BETWEEN 41 AND 50 THEN '41-50'
            WHEN age BETWEEN 51 AND 60 THEN '51-60'
            WHEN age BETWEEN 61 AND 70 THEN '61-70'
            ELSE '71+'
        END AS age_group,
        AVG(purchases) AS avg_purchases
    FROM customers
    GROUP BY age_group
""").fetchall()

# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group in DuckDB?
print("====================== Results ======================")
print("Total purchase amount by Gender: " + total_pruchases_by_gender)
print("Average purchase amount by Age group: " + average_purchase_by_age_group)
