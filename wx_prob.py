from pyspark import SparkContext
from pyspark.sql import SQLContext
import mbd_util as u
import ast

sc = SparkContext("local", "wx_prob")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)

df = sqlContext.read.json("hdfs:///user/s1638696/wx_data/*")

sigmets = df.flatMap(lambda r: r.features).filter(lambda r: r.properties.geom == "AREA")
sigmets = sigmets.filter(lambda s: type(ast.literal_eval(s.geometry.coordinates[0])) == list)
wx_blocks = sigmets.flatMap(lambda s: [(block, [s]) for block in u.get_covered_blocks(ast.literal_eval(s.geometry.coordinates[0]))])
wx_per_block = wx_blocks.reduceByKey(lambda a, b: a + b)
wx_time_per_block = wx_per_block.map(lambda (block, list_of_sigmets): (block, sum([s.properties.v_to -  s.properties.v_from for s in list_of_sigmets])))

