from pyspark import SparkContext
from pyspark.sql import SQLContext
import mbd_util as u


sc = SparkContext(appName="al_prob")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)

def aggregate_ac(agg, ac):
	agg += 1
	return agg

def combine_aggs(a1, a2):
	return a1 + a2

#df = sqlContext.read.json("file:///home/s1638696/flight_data/2017-12-19-small.tar.gz")
#ac_df = sqlContext.read.json("hdfs:///user/s1638696/flight_data/*")
ac_df = sqlContext.read.json("hdfs:///user/s1638696/flight_data/2017-01-01.tar.gz")
ac_df_filtered = ac_df.where( (ac_df.GAlt>0) & (ac_df.Lat.isNotNull()) & (ac_df.Long.isNotNull()) & (ac_df.Reg.isNotNull()) & (ac_df.Reg != '') & (ac_df.Reg != ac_df.Call) & (ac_df.PosStale.isNull()) & (ac_df.CallSus == False) & (ac_df.Op.isNotNull()))
airlines_per_block = ac_df_filtered.map(lambda r: (u.block_name(r.Lat, r.Long), 1))
# This should be aggregate by key. Aggregate operates on partitions instead of combining values accross partitions
ac_counts = airlines_per_block.aggregateByKey(0, aggregate_ac, combine_aggs)

#ac_counts.toDF().write.json("file:///home/s1542648/ac_count.json", mode="overwrite")
ac_counts.toDF().write.json("/user/s1638696/ac_output/ac_count.json", mode="overwrite")
