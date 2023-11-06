from mylib.lib import extract, load, describe, query, transform, start_spark, end_spark
from pyspark.sql import SparkSession

def main():
    extract()
    spark = start_spark("WorldCupPred")
    df = load(spark)
    describe(df)
    query(df, "world_cup_data")
    transform(df)
    end_spark(spark)

if __name__ == "__main__":
    main()