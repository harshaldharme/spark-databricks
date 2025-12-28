# Databricks notebook source
spark

# COMMAND ----------

data = [
{"id": 1, "name": "Alice", "age": 29},
{"id": 2, "name": "Bob", "age": 35}
]

source_data_df = spark.createDataFrame(data)
display(source_data_df)

# COMMAND ----------

data = [
    {"name": "Alice", "country": "USA", "salary": 50000},
    {"name": "Bob", "country": "Canada", "salary": 80000}
]

df = spark.createDataFrame(data)
display(df)

# COMMAND ----------

df = df.withColumnRenamed("name", "full_name")
display(df)

# COMMAND ----------

from pyspark.sql.functions import expr
df = df.withColumn("tax", expr("salary * 0.2"))
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, when
df = df \
    .withColumn("salary_bracket", when(col("salary") < 60000, "lower")\
                                    .when((col("salary") > 60000) & (col("salary") < 100000), "middle")\
                                    .otherwise("higher"))

df = df.withColumn("salary_bracket_alternate", expr("CASE WHEN salary < 60000 THEN 'lower' WHEN salary > 60000 AND salary < 100000 THEN 'middle' ELSE 'higher' END"))
df.show()

# COMMAND ----------

df_filtered = df.filter(col("salary") < 60000)
df_filtered_middle = df.filter((col("salary") > 60000) & (df.salary_bracket == 'middle'))
df_filtered.show()
df_filtered_middle.show()

# COMMAND ----------

df.show()
# df.filter(df.salary_bracket_alternate.like("LOWER")).show()
df.filter(col("salary_bracket_alternate").like("l%")).show()
df.filter(col("salary_bracket").startswith("m")).show()


# COMMAND ----------

df.filter(col("salary_bracket_alternate").endswith("r")).show()
df.filter(~col("salary_bracket_alternate").endswith("r")).show()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, min, max
df.count()
df.select(countDistinct("salary").alias("distinct_salary")).show()
df.select(min("salary").alias("min_salary"), max("salary").alias("max_salary")).show()

# COMMAND ----------

df_grouped = (
    df.groupBy("country")
        .agg(count("country").alias("Country_count"),
             sum("salary").alias("total_salary"))
)
df_grouped.show()

# COMMAND ----------

customers_data = [
    {"customer_id": 1, "name": "Alice", "country": "USA"},
    {"customer_id": 2, "name": "Bob", "country": "Canada"},
    {"customer_id": 3, "name": "Charlie", "country": "UK"},
    {"customer_id": 4, "name": "David", "country": "Germany"},
    {"customer_id": 5, "name": "Eva", "country": "France"},
    {"customer_id": 6, "name": "Frank", "country": "Italy"},
    {"customer_id": 7, "name": "Grace", "country": "Spain"},
    {"customer_id": 8, "name": "Helen", "country": "Australia"},
    {"customer_id": 9, "name": "Ian", "country": "Japan"},
    {"customer_id": 10, "name": "Julia", "country": "Brazil"},
    {"customer_id": 11, "name": "Kevin", "country": "India"},
    {"customer_id": 12, "name": "Laura", "country": "Mexico"}
]
orders_data = [
    {"order_id": 101, "customer_id": 1, "amount": 250},
    {"order_id": 102, "customer_id": 2, "amount": 400},
    {"order_id": 103, "customer_id": 3, "amount": 150},
    {"order_id": 104, "customer_id": 4, "amount": 300},
    {"order_id": 105, "customer_id": 5, "amount": 500},
    {"order_id": 106, "customer_id": 6, "amount": 200},
    {"order_id": 107, "customer_id": 7, "amount": 350},
    {"order_id": 108, "customer_id": 8, "amount": 450},
    {"order_id": 109, "customer_id": 9, "amount": 100},
    {"order_id": 110, "customer_id": 10, "amount": 600},
    {"order_id": 111, "customer_id": 11, "amount": 700},
    {"order_id": 112, "customer_id": 12, "amount": 800}
]

customers = spark.createDataFrame(customers_data)
orders = spark.createDataFrame(orders_data)
display(customers)
display(orders)

# COMMAND ----------

df_joined_inner = customers.join(orders, on=["customer_id"], how="inner")
df_joined_left_outer = customers.join(orders, how="left", on=["customer_id"])
df_joined_full_outer = customers.join(orders, how="outer", on=["customer_id"])
df_joined_left_semi = customers.join(orders, how="left_semi", on=["customer_id"])
df_joined_left_anti = customers.join(orders, how="left_anti", on=["customer_id"])

df_joined_left_anti.show()

# COMMAND ----------


