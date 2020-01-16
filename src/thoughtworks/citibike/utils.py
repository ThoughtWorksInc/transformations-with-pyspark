import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

meters_per_foot = 0.3048
feet_per_mile = 5280
earth_radius_in_meters = 6371e3
meters_per_mile = meters_per_foot * feet_per_mile

def computeDistances(spark, dataframe):
    df = dataframe.withColumn("lat1",F.radians(F.col("start_station_latitude"))).withColumn("lat2",F.radians(F.col("end_station_latitude")))\
        .withColumn("lon1",F.radians(F.col("start_station_longitude"))).withColumn("lon2",F.radians(F.col("end_station_longitude")))\
        .withColumn("distance",F.round(F.asin(F.sqrt(
            (-F.cos(F.col("lat2") - F.col("lat1"))*0.5 + 0.5) +
              F.cos(F.col("lat1"))*
                F.cos(F.col("lat2"))*
                (-F.cos(F.col("lon2") - F.col("lon1"))*0.5 + 0.5)))
                  *(2*earth_radius_in_meters/meters_per_mile),2))
    df = df.drop(F.col("lat1")).drop(F.col("lat2")).drop(F.col("lon1")).drop(F.col("lon2"))
    return df
