from mylib.lib import extract, load, query, transform, start_spark, end_spark

def main():
    extract()
    spark = start_spark("WorldCupPred")
    df = load(spark)
    query(spark, df)
    transform(df)
    end_spark(spark)

if __name__ == "__main__":
    main()