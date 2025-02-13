# Databricks notebook source
# DBTITLE 1,Polar Package Installation Command
# MAGIC %pip install polars

# COMMAND ----------

# MAGIC %pip install ../files/my_package-0.0.1-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

df = spark.table("main.imdb.movies")

# COMMAND ----------

# DBTITLE 1,Structured Dataframe Creation
df = spark.createDataFrame(
    [
      ("xyz",
        [
        {
            "name": "a",
            "v1": 1,
            "v2": 10
        },
        {
            "name": "b",
            "v1": 2,
            "v2": 20
        }
        
    ]),
      ("abc",
        [
        {
            "name": "a",
            "v1": 4,
            "v2": 40
        },
        {
            "name": "b",
            "v1": 3,
            "v2": 30
        }
    ])      
    ],
    'id: string, attributes: array<struct<name:string,v1:int,v2:int>>'

)

df.createOrReplaceTempView("df")

# COMMAND ----------

# DBTITLE 1,Spark Attribute Exploder Display
from pyspark.sql.functions import *

transformed = (
    df.withColumn("attributes", explode(col("attributes")))
    .select("*", explode(from_json(to_json(col("attributes")), "map<string, string>")))
    .withColumn(
        "attribute",
        when(
            col("key") != "name",
            create_map(
              concat_ws(".", lit("attributes"), col("attributes.name"), col("key")), col("value")
            )
        ),
    )
).filter("attribute is not null").select("id", "attribute")

display(transformed)
