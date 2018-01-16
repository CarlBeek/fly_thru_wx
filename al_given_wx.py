from pyspark import SparkContext
from pyspark.sql import SQLContext
import mbd_util as u
import ast
import itertools


# iterate over blocks
# 
sc = SparkContext(appName= "al_given_wx")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)

df = sqlContext.read.json("file:///home/s1638696/flight_data/wx_data_fixed/*")
#df = sqlContext.read.json("hdfs:///user/s1638696/wx_data/*")

sigmets = df.flatMap(lambda r: r.features).filter(lambda r: r.properties.geom == "AREA")
sigmets = sigmets.filter(lambda s: type(ast.literal_eval(s.geometry.coordinates[0])) == list)
wx_blocks = sigmets.flatMap(lambda s: [(block, [s]) for block in u.get_covered_blocks(ast.literal_eval(s.geometry.coordinates[0]))])
wx_per_block = wx_blocks.reduceByKey(lambda a, b: a + b)

#wx = dict(wx_per_block.collect())
    

#ac_df = sqlContext.read.json("hdfs:///user/s1638696/flight_data/*")
#ac_df = sqlContext.read.json("hdfs:///user/s1638696/small_ac_data.tar.gz")
ac_df = sqlContext.read.json("file:///home/s1638696/flight_data/2017-12-19-small.tar.gz")


def flightdata_to_tuple(data):
    block_name = u.block_name(data.Lat, data.Long)
    return (block_name, [(data.Lat, data.Long, data.GAlt, data.PosTime, data.Op)])

def check_poly_fn(block_name, d, w):
    weather_type = w['properties']['hazard'] or ""
    w_top = w['properties']['top'] 
    w_base = w['properties']['base']
    if w_base == w_top or w_top==0:
        w_top = 100000
    w_from = w['properties']['v_from']
    w_to = w['properties']['v_to']
    lat = d[0]
    long = d[1]
    gAlt = d[2]
    posTime = d[3]
    op = d[4]
    isInside = u.inside_polygon(lat, long, w['geometry']) and w_top > gAlt and w_base < gAlt \
     and posTime>w_from and posTime<w_to 
    if isInside:
        return ((weather_type, block_name, op), 1)

def check_poly(block_name, pair):
    weather = pair[1]
    data = pair[0]
    if data is None or weather is None:
        return []
    #return [(block_name, [len(weather), len(data)])]
    res = [[check_poly_fn(block_name, d, w) for d in data] for w in weather]
    return list(itertools.chain.from_iterable(res))
   


big = (ac_df.where( (ac_df.GAlt>0) & (ac_df.Lat.isNotNull()) & (ac_df.Long.isNotNull()) & (ac_df.Reg.isNotNull()) & (ac_df.Reg != '') & (ac_df.Reg != ac_df.Call) & (ac_df.PosStale.isNull()) & (ac_df.CallSus == False))
    .map(flightdata_to_tuple)
    .reduceByKey(lambda a, b: a+b )
    .join(wx_per_block))

count_per_wx_block_op = big.flatMap(lambda kv: check_poly(kv[0], kv[1])) \
    .reduceByKey(lambda a, b: a+b)

count_per_wx_block_op.toDF().write.json("./al_given_wx.json")
#print(count_per_wx_block_op.collect())