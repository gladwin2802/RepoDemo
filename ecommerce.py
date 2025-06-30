# COMMAND ----------
# First, import and create SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Use ecommerce database
spark.sql("USE ecommerce")
spark.sql("SHOW TABLES").show()

# COMMAND ----------
# Preview tables
print("Customers preview:")
spark.sql("select * from customers limit 5").show()
print("Orders preview:")
spark.sql("select * from orders limit 5").show()
print("Products preview:")
spark.sql("select * from products limit 5").show()
print("Sales preview:")
spark.sql("select * from sales limit 5").show()

# COMMAND ----------
# Count records
print("Customer count:")
spark.sql("select count(*) from customers").show()
print("Orders count:")
spark.sql("select count(*) from orders").show()
print("Products count:")
spark.sql("select count(*) from products").show()
print("Sales count:")
spark.sql("select count(*) from sales").show()

# COMMAND ----------
# Describe tables
print("Customers schema:")
spark.sql("desc customers").show()
print("Orders schema:")
spark.sql("desc orders").show()
print("Products schema:")
spark.sql("desc products").show()
print("Sales schema:")
spark.sql("desc sales").show()

# COMMAND ----------
# Gender analysis
print("Customer gender distribution:")
spark.sql("""
SELECT gender, COUNT(*) AS total_customers
FROM customers
GROUP BY gender
""").show()

# COMMAND ----------
# State analysis
print("Customer state distribution:")
spark.sql("""
SELECT state, COUNT(*) AS total_customers
FROM customers
GROUP BY state
""").show()

# COMMAND ----------
# Top customers by spend
print("Top 10 customers by spend:")
spark.sql("""
SELECT c.customer_id, c.name, SUM(o.payment) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_spent DESC
LIMIT 10
""").show()

# COMMAND ----------
# Total orders and revenue
print("Total orders and revenue:")
spark.sql("""
SELECT COUNT(*) AS total_orders, ROUND(SUM(payment), 2) AS total_revenue
FROM orders
""").show()

# COMMAND ----------
# Daily revenue
print("Daily revenue:")
spark.sql("""
SELECT order_date, SUM(payment) AS daily_revenue
FROM orders
GROUP BY order_date
ORDER BY order_date
""").show()

# COMMAND ----------
# Average order value
print("Average order value:")
spark.sql("""
SELECT AVG(payment) AS average_order_value
FROM orders
""").show()

# COMMAND ----------
# Top products by quantity
print("Top 10 products by quantity sold:")
spark.sql("""
SELECT p.product_id, p.product_name, SUM(s.quantity) AS total_sold
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_sold DESC
LIMIT 10
""").show()

# COMMAND ----------
# Products by revenue
print("Products by revenue:")
spark.sql("""
SELECT p.product_id, p.product_name, SUM(s.total_price) AS revenue_generated
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY revenue_generated DESC
""").show()

# COMMAND ----------
# Low stock products
print("Low stock products:")
spark.sql("""
SELECT product_id, product_name, stock_quantity
FROM products
WHERE stock_quantity < 10
""").show()

# COMMAND ----------
# Average units per order
print("Average units per order:")
spark.sql("""
SELECT AVG(s.quantity) AS avg_units_per_order
FROM sales s
""").show()

# COMMAND ----------
# Sales by category
print("Sales by category:")
spark.sql("""
SELECT p.category, SUM(s.total_price) AS total_sales
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC
""").show()

# COMMAND ----------
# Multiple order customers
print("Customers with multiple orders:")
spark.sql("""
SELECT customer_id, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY order_count DESC
""").show()

# COMMAND ----------
# Customer lifetime value
print("Customer lifetime value:")
spark.sql("""
WITH avg_order_value AS (
  SELECT AVG(payment) AS aov FROM orders
),
purchase_frequency AS (
  SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.name,
  ROUND(aov * pf.order_count, 2) AS estimated_clv
FROM customers c
JOIN purchase_frequency pf ON c.customer_id = pf.customer_id
CROSS JOIN avg_order_value
""").show()

# COMMAND ----------
# Inactive customers
print("Inactive customers:")
spark.sql("""
SELECT
  c.customer_id,
  c.name,
  MAX(o.order_date) AS last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
HAVING MAX(o.order_date) < CURRENT_DATE() - INTERVAL 60 DAYS
""").show()
