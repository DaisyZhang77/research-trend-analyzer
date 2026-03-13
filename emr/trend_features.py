from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

def main():
    spark = SparkSession.builder.appName("ResearchTrendFeatures").getOrCreate()


    input_path = "s3://research-trend-analyzer/raw/openalex/"
    output_path = "s3://research-trend-analyzer/features/topic_trends/"

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("publication_date", StringType(), True),
    ])

    df = spark.read.schema(schema).json(input_path)

    df = df.select("id", "topic", "publication_date")
    df = df.filter(F.col("topic").isNotNull())
    df = df.filter(F.col("publication_date").isNotNull())

    df = df.withColumn("publication_date", F.to_date("publication_date"))
    df = df.dropDuplicates(["id"])

    daily_counts = (
        df.groupBy("topic", "publication_date")
          .agg(F.count("*").alias("paper_count"))
    )

    topic_window = Window.partitionBy("topic").orderBy("publication_date")

    daily_counts = daily_counts.withColumn(
        "prev_count",
        F.lag("paper_count").over(topic_window)
    )

    daily_counts = daily_counts.withColumn(
        "growth_rate",
        (F.col("paper_count") - F.col("prev_count")) /
        F.when(F.col("prev_count") > 0, F.col("prev_count")).otherwise(1)
    )

    #recent 3 days
    rolling_window = (
        Window.partitionBy("topic")
              .orderBy("publication_date")
              .rowsBetween(-2, 0)
    )
    #calculate average paper count in recent 3 days
    daily_counts = daily_counts.withColumn(
        "moving_avg_3d",
        F.avg("paper_count").over(rolling_window)
    )

    #normalize the score for growth rate
    daily_counts = daily_counts.withColumn(
        "emerging_score",
        F.col("growth_rate") * F.log1p(F.col("paper_count"))
    )

    daily_counts.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()