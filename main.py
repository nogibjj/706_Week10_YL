from mylib.lib import extract, load, describe, query, transform
from pyspark.sql import SparkSession

def main():
    extract()
    spark = SparkSession.builder.appName("WorldCupPred").getOrCreate()
    df = load(spark)
    describe(df)
    query(df, "world_cup_data")
    transform(df)
    spark.stop()

if __name__ == "__main__":
    main()