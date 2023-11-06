import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType

def extract(url=" https://github.com/fivethirtyeight/data/blob/15f210532b2a642e85738ddefa7a2945d47e2585/world-cup-predictions/wc-20140609-140000.csv?raw=True",
            file_path="data/wc-20140609-140000.csv",
            directory="data"):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    
    return file_path

def load(spark, data="wc-20140609-140000.csv", name="WorldCupPred"):
    schema = StructType([
        StructField("country", StringType(), True),
        StructField("country_id", StringType(), True),
        StructField("group", StringType(), True),
        StructField("spi", FloatType(), True),
        StructField("spi_offense", FloatType(), True),
        StructField("spi_defense", FloatType(), True),
        StructField("win_group", FloatType(), True),
        StructField("sixteen", FloatType(), True),
        StructField("quarter", FloatType(), True),
        StructField("semi", FloatType(), True),
        StructField("cup", FloatType(), True),
        StructField("win", FloatType(), True)
    ])
    df = spark.read.option("header", "true").schema(schema).ccv(data)

    return df

def describe(df):
    return df.describe().show()

def query(df, name):
    spark = SparkSession.builder.appName("WorldCupPred").getOrCreate()
    df = df.createOrReplaceTempView(name)
    res = spark.sql("""SELECT group,
                    AVG(spi) AS avg_power_per_group,
                    COUNT(win) AS win_odds,
                    FROM world_cup_data
                    GROUP BY group""")
    
    return res.show()

def transform(df):
    USA = df.filter(df["country"] == "USA")
    return USA.show()