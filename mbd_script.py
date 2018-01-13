from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext("local", "flight_data")
sqlContext = SQLContext(sc)

import math
sc.setLogLevel("ERROR")
#df = sqlc.read.json("hdfs:///user/s1638696/flight_data/2017-12-19.tar.gz")
df = sqlContext.read.json("hdfs:///user/s1638696/small_ac_data.tar.gz")
df1 = df.where( (df.GAlt>0) & (df.Lat.isNotNull()) & (df.Long.isNotNull()) & (df.Reg.isNotNull()) & (df.Reg != '') & (df.Reg != df.Call) & (df.PosStale.isNull()) & (df.CallSus == False))

df2 = df1.distinct(lambda row: time(row.PosTime))


def time(timestamp):
    return timestamp / 60000

def block_name(lat, lon):
    discretized_lat = math.floor(lat)+0.5
    discretized_lon = math.floor(lon)+0.5
    return (discretized_lat, discretized_lon)

df2 = df1.map(lambda v: (block_name(v.Lat, v.Long), [[v]]))
df3 = df2.reduceByKey(lambda a, b: a+b)
