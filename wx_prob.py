from pyspark import SparkContext
from pyspark.sql import SQLContext
import mbd_util as u
import ast

sc = SparkContext(appName="wx_prob")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)

#df = sqlContext.read.json("file:///home/s1542648/wx_data/20171229.json")
df = sqlContext.read.json("hdfs:///user/s1638696/wx_data/*")
#df = sqlContext.read.json("file:///home/s1638696/flight_data/wx_data_fixed/*")
#df = sqlContext.read.json("hdfs:///user/s1638696/us_wx_data/*")

def calc_wx_prob(kv):
	block, t = kv
	return block, t/(365*24*3600*1000.0)

def is_area(area_str):
	l = ast.literal_eval(area_str.geometry.coordinates[0])
	if not type(l) == list:
		return False
	if not type(l[0]) == list:
		return False
	return True

sigmets = df.flatMap(lambda r: r.features).filter(lambda r: not (r.properties.v_from is None or r.properties.v_to is None))
sigmets = sigmets.filter(is_area)
wx_blocks = sigmets.flatMap(lambda s: [(block, [s]) for block in u.get_covered_blocks(ast.literal_eval(s.geometry.coordinates[0]))])
wx_per_block = wx_blocks.reduceByKey(lambda a, b: a + b)
wx_time_per_block = wx_per_block.map(lambda (k, list_of_sigmets): (k, sum([s.properties.v_to -  s.properties.v_from for s in list_of_sigmets])))
wx_prob = wx_time_per_block.map(calc_wx_prob)
wx_prob.toDF().write.json("/user/s1638696/ac_output/wx_prob_updated.json", mode="overwrite")
#wx_prob.toDF().write.json("file:///home/s1542648/wx_data/output", mode="overwrite")

