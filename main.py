import sqlite3
import pandas as pd
conn = sqlite3.connect('data.sqlite')

# Counting the number of customers in each country
q = """
SELECT country, COUNT(*)
FROM customers
GROUP BY country
;
"""
# Displaying just the first 10 countries for readability
customers_count_by_country = pd.read_sql(q, conn).head(10)
print(customers_count_by_country)
print("\n")


# Alternative way to write the query above
q = """
SELECT country, COUNT(*)
FROM customers
GROUP BY 1
;
"""
# Displaying just the first 10 countries for readability
customers_count_by_country = pd.read_sql(q, conn).head(10)
print(customers_count_by_country)
print("\n")


# Same as above, but using the alias for the count column
q = """
SELECT country, COUNT(*) AS customer_count
FROM customers
GROUP BY country
;
"""
# Displaying just the first 10 countries for readability
customers_count_by_country = pd.read_sql(q, conn).head(10)
print(customers_count_by_country)
print("\n")


# Summarizing payment information for each customer
q = """
SELECT
   customerNumber,
   COUNT(*) AS number_payments,
   MIN(CAST(amount AS FLOAT)) AS min_purchase,
   MAX(CAST(amount AS FLOAT)) AS max_purchase,

AVG(CAST(amount AS FLOAT)) AS avg_purchase,
   SUM(CAST(amount AS FLOAT)) AS total_spent
FROM payments
GROUP BY customerNumber
;
"""
payment_summary = pd.read_sql(q, conn)
print(payment_summary)
print("\n")


# Summarizing payment information for each customer, but only for payments made in 2004
q = """
SELECT
   customerNumber,
   COUNT(*) AS number_payments,
   MIN(CAST(amount AS FLOAT)) AS min_purchase,
   MAX(CAST(amount AS FLOAT)) AS max_purchase,
   AVG(CAST(amount AS FLOAT)) AS avg_purchase,
   SUM(CAST(amount AS FLOAT)) AS total_spent
FROM payments
WHERE strftime('%Y', paymentDate) = '2004'
GROUP BY customerNumber
;
"""
payment_summary_2024 = pd.read_sql(q, conn)
print(payment_summary_2024)
print("\n")


# Summarizing payment information for each customer,
# but only for customers whose average purchase is greater than 50,000
q = """
SELECT
   customerNumber,
   COUNT(*) AS number_payments,
   MIN(CAST(amount AS FLOAT)) AS min_purchase,
   MAX(CAST(amount AS FLOAT)) AS max_purchase,
   AVG(CAST(amount AS FLOAT)) AS avg_purchase,
   SUM(CAST(amount AS FLOAT)) AS total_spent
FROM payments
GROUP BY customerNumber
HAVING avg_purchase > 50000
;
"""
payments_over_fifty_thousand = pd.read_sql(q, conn)
print(payments_over_fifty_thousand)
print("\n")


# Same as above, but using the full expression for the average purchase in the HAVING clause instead of the alias
# Note: This is necessary because some SQL databases do not allow the use of column aliases in the HAVING clause
q = """
SELECT
   customerNumber,
   COUNT(*) AS number_payments,
   MIN(CAST(amount AS FLOAT)) AS min_purchase,
   MAX(CAST(amount AS FLOAT)) AS max_purchase,
   AVG(CAST(amount AS FLOAT)) AS avg_purchase,
   SUM(CAST(amount AS FLOAT)) AS total_spent
FROM payments
GROUP BY customerNumber
HAVING AVG(CAST(amount AS FLOAT)) > 50000
;
"""
payments_over_fifty_thousand = pd.read_sql(q, conn)
print(payments_over_fifty_thousand)
print("\n")


# Summarizing payment information for each customer,
# but only for customers who made at least two purchases over 50,000
q = """
SELECT
   customerNumber,
   COUNT(*) AS number_payments,
   MIN(CAST(amount AS FLOAT)) AS min_purchase,
   MAX(CAST(amount AS FLOAT)) AS max_purchase,
   AVG(CAST(amount AS FLOAT)) AS avg_purchase,
   SUM(CAST(amount AS FLOAT)) AS total_spent
FROM payments
WHERE CAST(amount AS FLOAT) > 50000
GROUP BY customerNumber
HAVING number_payments >= 2
;
"""
multiple_purchase_payment_summary = pd.read_sql(q, conn)
print(multiple_purchase_payment_summary)
print("\n")


# Finding the customer who spent the least among those who made at least two purchases over 50,000
q = """
SELECT
   customerNumber,
   COUNT(*) AS number_payments,
   MIN(CAST(amount AS FLOAT)) AS min_purchase,
   MAX(CAST(amount AS FLOAT)) AS max_purchase,
   AVG(CAST(amount AS FLOAT)) AS avg_purchase,
   SUM(CAST(amount AS FLOAT)) AS total_spent
FROM payments
WHERE CAST(amount AS FLOAT) > 50000
GROUP BY customerNumber
HAVING number_payments >= 2
ORDER BY total_spent
LIMIT 1
;
"""
lowest_duplicate_spender = pd.read_sql(q, conn)
print(lowest_duplicate_spender)
print("\n")

conn.close()