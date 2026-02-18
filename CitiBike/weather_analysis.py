from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, to_timestamp, when, date_format, count, avg, round as spark_round, concat_ws, lpad, from_utc_timestamp
from citibike import initialize_dataframe as citibike_init, generate_additional_columns


def initialize_dataframe(spark : SparkSession, path : str) -> DataFrame:

    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("temp", DoubleType(), True),
        StructField("temp_source", StringType(), True),
        StructField("rhum", DoubleType(), True),
        StructField("rhum_source", StringType(), True),
        StructField("prcp", DoubleType(), True),
        StructField("prcp_source", StringType(), True),
        StructField("wdir", DoubleType(), True),
        StructField("wdir_source", StringType(), True),
        StructField("wspd", DoubleType(), True),
        StructField("wspd_source", StringType(), True),
        StructField("wpgt", DoubleType(), True),
        StructField("wpgt_source", StringType(), True),
        StructField("pres", DoubleType(), True),
        StructField("pres_source", StringType(), True),
        StructField("cldc", DoubleType(), True),
        StructField("cldc_source", StringType(), True),
        StructField("coco", DoubleType(), True),
        StructField("coco_source", StringType(), True)
    ])
    
    df = (spark.read
          .option("header", "true")
          .schema(schema)
          .csv(path))
    
    df_clean = df.filter(
        (col("temp").isNotNull()) & 
        (col("year") == 2024) &
        (col("month") == 10)
    )
    
    df_clean = df_clean.withColumn("hour_timestamp_utc",
                                   to_timestamp(
                                       concat_ws("-",
                                                col("year"),
                                                lpad(col("month"), 2, "0"),
                                                lpad(col("day"), 2, "0"),
                                                lpad(col("hour"), 2, "0")),
                                       "yyyy-MM-dd-HH"))
    
    df_clean = df_clean.withColumn("hour_timestamp",
                                   from_utc_timestamp(col("hour_timestamp_utc"), "America/New_York"))
    
    # Indikator padavina baziran na coco koloni (weather condition code)
    # coco kodovi:
    # 7=Light Rain, 8=Rain, 9=Heavy Rain, 10=Freezing Rain, 11=Heavy Freezing Rain
    # 12=Sleet, 13=Heavy Sleet, 14=Light Snowfall, 15=Snowfall, 16=Heavy Snowfall
    # 17=Rain Shower, 18=Heavy Rain Shower, 19=Sleet Shower, 20=Heavy Sleet Shower
    # 21=Snow Shower, 22=Heavy Snow Shower
    # Kategorije:
    # "clear": ne nuzno clear, ali nije opasno
    # "light_precip": lagane padavine (7, 14)
    # "moderate_precip": umjerene padavine (8, 15, 17, 21)
    # "heavy_precip": jake padavine (9, 16, 18, 22)
    # "hazardous": opasni uslovi (0, 11, 12, 13, 19, 20)
    df_clean = df_clean.withColumn("rain_indicator",
                                   when(col("coco").isin([7, 14]), "light_precip")
                                   .when(col("coco").isin([8, 15, 17, 21]), "moderate_precip")
                                   .when(col("coco").isin([9, 16, 18, 22]), "heavy_precip")
                                   .when(col("coco").isin([10, 11, 12, 13, 19, 20]), "hazardous")
                                   .otherwise("clear"))
    
    # Temperaturni bandovi(temp je u celz)
    # Kategorije
    # "very_cold": < 5
    # "cold": 5-10
    # "mild": 10-15 
    # "warm": 15-20
    #  "hot": >= 20
    df_clean = df_clean.withColumn("temp_band",
                                   when(col("temp") < 5, "very_cold")
                                   .when(col("temp") <= 10, "cold")
                                   .when(col("temp") <= 20, "mild")
                                   .when(col("temp") < 30, "warm")
                                   .otherwise("hot"))
        
    return df_clean.cache()

def weather_impact_analysis(citibike_df : DataFrame, weather_df : DataFrame):
    citibike_with_hour = citibike_df.withColumn("hour_timestamp",
                                                 to_timestamp(date_format(col("started_at"), "yyyy-MM-dd HH:00:00")))
    
    joined_df = citibike_with_hour.join(
        weather_df.select("hour_timestamp", "rain_indicator", "temp", "temp_band"),
        on="hour_timestamp"
    )
    
    print(f"\nJoined dataset: {joined_df.count()} rides")
    
    joined_df = joined_df.withColumn("is_rainy",
                                     when(col("rain_indicator") != "clear", "rainy")
                                     .otherwise("non_rainy"))
    
    
    print("Uticaj padavina kontrolisan po satu u danu ===")
    by_hour = (joined_df
               .groupBy("hour_in_day", "is_rainy")
               .agg(
                   count("*").alias("num_rides"),
                   spark_round(avg("duration_minutes"), 2).alias("avg_duration"),
               )
               .orderBy("hour_in_day", "is_rainy")
    )
    by_hour.show(100, truncate=False)
    
    print("Uticaj padavina po tipu korisnika")
    by_member = (joined_df
                 .groupBy("hour_in_day", "member_casual", "is_rainy")
                 .agg(
                     count("*").alias("num_rides"),
                     spark_round(avg("duration_minutes"), 2).alias("avg_duration")
                 )
                 .orderBy("hour_in_day", "member_casual", "is_rainy")
    )
    by_member.show(100, truncate=False)
   
    return joined_df, by_hour, by_member

def temperature_impact_analysis(joined_df : DataFrame):

    print("Razlika member i casual po temperaturi")
    temp_diff = (joined_df
                        .groupBy("temp_band")
                        .agg(
                            count(when(col("member_casual") == "member", 1)).alias("member_rides"),
                            count(when(col("member_casual") == "casual", 1)).alias("casual_rides"),
                            spark_round(avg(when(col("member_casual") == "casual", col("duration_minutes"))), 2).alias("casual_avg_duration")
                        )
                        .withColumn("total_rides", col("member_rides") + col("casual_rides"))
                        .withColumn("member_percentage", spark_round((col("member_rides") / col("total_rides")) * 100, 2))
                        .withColumn("casual_percentage", spark_round((col("casual_rides") / col("total_rides")) * 100, 2))
                        .orderBy("temp_band")
    )
    temp_diff.show(50, truncate=False)
    
    return temp_diff

def main():
    spark = SparkSession.builder.appName("Weather Analysis").config("spark.driver.memory", "16g").getOrCreate()
        
    weather_df = initialize_dataframe(spark, "data/KJRB0.csv")
    
    citibike_df = citibike_init(spark, "data/tripdata/*.csv")
    citibike_df = generate_additional_columns(citibike_df)
    
    joined_df, by_hour, by_member = weather_impact_analysis(citibike_df, weather_df)
    
    output_dir = "Weather_Analysis"

    by_hour.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/1_rain_impact_by_hour")
    
    by_member.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/2_rain_impact_by_member")
    
    print("Analiza uticaja temperature")
    temp_diff = temperature_impact_analysis(joined_df)
    
    temp_diff.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_dir}/3_temp_impact_comparison")

if __name__ == "__main__":
    main()