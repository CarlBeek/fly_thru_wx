from pyspark import SparkContext
from pyspark.sql import SQLContext
import mbd_util as u


sc = SparkContext(appName="al_prob")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)

def aggregate_airlines(agg, airline):
	if airline in agg:
		agg[airline] += 1
	else:
		agg[airline] = 1
	return agg
	
def al_probs(b, d):
	# Gets a dictionary with AL Op names per block as d
	# Returns list of tuples key, value where key is a tuple block_name, Op string and value is prob
	output = []
	b_lat, b_lon = b
	ac_in_b = 0.0
	for k, v in d.items():
		ac_in_b += v	
	for k, v in d.items():
		output.append( (((b_lat, b_lon), k), v/ac_in_b) )
	return output

def combine_dicts(d1, d2):
	for k, v in d2.items():
		if k in d1:
			d1[k] += d2[k]
		else:
			d1[k] = d2[k]
	return d1

#ac_df = sqlContext.read.json("file:///home/s1638696/flight_data/2017-12-19-small.tar.gz")
ac_df = sqlContext.read.json("hdfs:///user/s1638696/flight_data/*")
ac_df_filtered = ac_df.where( (ac_df.GAlt>0) & (ac_df.Lat.isNotNull()) & (ac_df.Long.isNotNull()) & (ac_df.Reg.isNotNull()) & (ac_df.Reg != '') & (ac_df.Reg != ac_df.Call) & (ac_df.PosStale.isNull()) & (ac_df.CallSus == False) & (ac_df.Op.isNotNull()))
airlines_per_block = ac_df_filtered.map(lambda r: (u.block_name(r.Lat, r.Long), r.Op))
# This should be aggregate by key. Aggregate operates on partitions instead of combining values accross partitions
red_al = airlines_per_block.aggregateByKey(dict(), aggregate_airlines, combine_dicts)
al_prob = red_al.flatMap(lambda (b, d): al_probs(b,d)) 

#al_prob.toDF().write.json("file:///home/s1542648/al_prob.json", mode="overwrite")
al_prob.toDF().write.json("/user/s1638696/ac_output/al_prob.json", mode="overwrite")
