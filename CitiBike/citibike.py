from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, when, date_format, hour, count, avg, expr, round as spark_round

def initialize_dataframe(spark : SparkSession, path : str) -> DataFrame :
    schema = StructType([
        StructField("ride_id", StringType(), False),
        StructField("rideable_type", StringType(), True),
        StructField("started_at", StringType(), True),
        StructField("ended_at", StringType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_id", StringType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_id", StringType(), True),
        StructField("start_lat", DoubleType(), True),
        StructField("start_lng", DoubleType(), True),
        StructField("end_lat", DoubleType(), True),
        StructField("end_lng", DoubleType(), True),
        StructField("member_casual", StringType(), True)
    ])

    df = (spark.read
        .option("header", "true")
        .schema(schema)
        .csv(path))

    print(f"Loaded : {df.count()} rows")

    # timestamp cols
    df = (df.withColumn("started_at", to_timestamp(col("started_at")))
            .withColumn("ended_at", to_timestamp(col("ended_at")))
        )

    # ride length
    df = df.withColumn("duration_minutes", (unix_timestamp("ended_at") - unix_timestamp("started_at")) / 60)

    df_clean = df.filter(
        (col("ride_id").isNotNull()) &
        (col("started_at").isNotNull()) &
        (col("start_station_id").isNotNull()) &
        (col("ended_at").isNotNull()) &
        
        (col("ended_at") > col("started_at")) &
        
        # assuming ride length greater than 24 hours is considered extreme
        (col("duration_minutes") > 0) &
        (col("duration_minutes") <= 1440)

    )

    return df_clean.cache()

def generate_additional_columns(df : DataFrame) -> DataFrame:
    # Ride length already added above

    # Add hour of day (0-23)
    df = df.withColumn("hour_in_day", hour(col("started_at")))

    # Add day 
    df = df.withColumn("day_of_week", date_format(col("started_at"), "EEEE"))

    df = df.withColumn("weekend_indicator", when((col("day_of_week") == "Saturday") | (col("day_of_week") == "Sunday"), True).otherwise(False))

    # Round trip flag
    df = df.withColumn("round_trip", when((col("start_station_id") == col("end_station_id")), True).otherwise(False))

    # Additional column -> ride_type with following boundaries:
    # Short : 0-15 minutes
    # Medium: 15-45 minutes
    # Long: 45+ minutes
    df = df.withColumn("ride_type", 
                       when(col("duration_minutes") <= 15, "short")
                       .when(col("duration_minutes") <= 45, "medium")
                       .otherwise("long"))

    return df.cache()

# prva stavka za 2 boda
def num_of_rides_and_len_analyze(df : DataFrame):
    print("Analiza broja voznji i tipicnog trajanja")
    
    result = (df
        .groupBy("weekend_indicator", "member_casual", "hour_in_day", "ride_type")
        .agg(
            count("*").alias("num_of_rides")
        )
        .orderBy("weekend_indicator", "member_casual", "hour_in_day", "ride_type")
    )
    
    result.show(300 ,truncate=False)
    return result
    

# druga stavka za 2 boda
def additional_station_metrics(n : int, df : DataFrame, min_rides : int = 100, order_by : str = "member_ratio"):
    result = (df
              .groupBy("start_station_name", "start_station_id")
              .agg(
                  count("*").alias("total_rides"),
                  count(when(col("member_casual") == "member", 1)).alias("member_rides"),
                  count(when(col("member_casual") == "casual", 1)).alias("casual_rides"),
                  count(when(col("round_trip") == True, 1)).alias("round_trip_rides"),
                  count(when(col("ride_type") == "short", 1)).alias("short_rides"),
                  count(when(col("ride_type") == "medium", 1)).alias("medium_rides"),
                  count(when(col("ride_type") == "long", 1)).alias("long_rides"),
                  spark_round(avg("duration_minutes"), 2).alias("avg_duration")
              )
              .filter(col("total_rides") >= min_rides)
              .withColumn("member_ratio", spark_round(col("member_rides") / col("total_rides"), 2))
              .withColumn("round_trip_ratio", spark_round(col("round_trip_rides") / col("total_rides"), 2))
              .withColumn("short_ratio", spark_round(col("short_rides") / col("total_rides"), 2))
              .withColumn("medium_ratio", spark_round(col("medium_rides") / col("total_rides"), 2))
              .withColumn("long_ratio", spark_round(col("long_rides") / col("total_rides"), 2))
              .orderBy(col(order_by).desc())
              .limit(n)
    )
    
    result.show(n, truncate=False)
    
    return result

#treca stavka za 2b
def most_common_routes(n : int, df: DataFrame, min_rides : int = 100):
    result = (df.groupBy("start_station_name", "end_station_name")
              .agg(
                  count("*").alias("total_rides"),
                  spark_round(expr("percentile_approx(duration_minutes, 0.5)"), 2).alias("median_duration"),
                  spark_round(avg("duration_minutes"), 2).alias("avg_duration"),
                  spark_round(expr("stddev(duration_minutes)"), 2).alias("stddev_duration"),
              )
              .filter(col("total_rides") >= min_rides)
              .withColumn("coef_variation", spark_round((col("stddev_duration") / col("avg_duration")) * 100, 2))
              .orderBy(col("total_rides").desc())
              .limit(n)
    )
    
    result.show(n, truncate=False)
    return result

#cetvrta stavka za 2b
def round_trip_analysis(df : DataFrame):

    total_rides = df.count()
    overall = (df.groupBy("round_trip")
               .agg(
                   count("*").alias("num_rides"),
                   spark_round(expr("percentile_approx(duration_minutes, 0.5)"), 2).alias("median_duration"),
                   spark_round(avg("duration_minutes"), 2).alias("avg_duration")
               )
               .withColumn("percentage", spark_round((col("num_rides") / total_rides) * 100, 2))
               .orderBy("round_trip")
    )
    overall.show(truncate=False)
    
    print("member vs casual\n")
    by_member = (df.groupBy("member_casual", "round_trip")
                 .agg(
                     count("*").alias("num_rides"),
                     spark_round(expr("percentile_approx(duration_minutes, 0.5)"), 2).alias("median_duration"),
                     spark_round(avg("duration_minutes"), 2).alias("avg_duration")
                 )
                 
    )
    
    by_member.show(truncate=False)
    
    return overall, by_member

def main():
    spark = SparkSession.builder.appName("CitiBike Data Analysis").config("spark.driver.memory", "16g").getOrCreate()

    df = initialize_dataframe(spark, "data/tripdata/*.csv")
    df = generate_additional_columns(df)
    
    output_dir = "CitiBike_Analysis"
    
    print("Broj voznji po satu, danu i tipu")
    result1 = num_of_rides_and_len_analyze(df)
    result1.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/1_rides_by_hour_type")
    
    print("Top stanice po omjeru clanova")
    result2a = additional_station_metrics(10, df, min_rides=100, order_by="member_ratio")
    result2a.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/2a_stations_by_member_ratio")
    
    print("Top stanice po omjeru round trip")
    result2b = additional_station_metrics(10, df, min_rides=100, order_by="round_trip_ratio")
    result2b.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/2b_stations_by_roundtrip_ratio")
    
    print("Najaktivnije stanice")
    result2c = additional_station_metrics(10, df, min_rides=100, order_by="total_rides")
    result2c.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/2c_stations_by_total_rides")
    
    print("Najcesce relacije")
    result3 = most_common_routes(10, df, min_rides=100)
    result3.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/3_most_common_routes")
    
    print("Round trip analiza")
    result4_overall, result4_by_member = round_trip_analysis(df)
    result4_overall.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/4a_roundtrip_overall")
    result4_by_member.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/4b_roundtrip_by_member")
    

if __name__ == "__main__":
    main()