from pyspark import SparkContext
from pyspark.sql import SQLContext
import mbd_util as u

def reduce_airlines(agg, new_value):
	for k, v in new_value.items():
		if k in agg:
			agg[k] += v
		else:
			agg[k] = v
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

df = sqlContext.read.json("hdfs:///user/s1638696/small_ac_data.tar.gz")
airlines_per_block = df.rdd.filter(lambda r: not (r.Lat is None or r.Long is None or r.Op is None)).map(lambda r: (u.block_name(r.Lat, r.Long), {r.Op: 1}))
red_al = airlines_per_block.reduceByKey(reduce_airlines)
al_prob = red_al.flatMap(lambda (b, d): al_probs(b,d)) 
