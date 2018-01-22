from pyspark import SparkContext
from pyspark.sql import SQLContext
import mbd_util as u
import ast
import itertools

USE_FARM = False
USE_POLYGON_CHECK = True

# iterate over blocks
# 
sc = SparkContext(appName= "al_given_wx")
#sc = SparkContext("local")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)


if USE_FARM:
    df = sqlContext.read.json("file:///home/s1638696/flight_data/wx_data_fixed/*")
    ac_df = sqlContext.read.json("file:///home/s1638696/flight_data/2017-12-19-small.tar.gz")
else:
    df = sqlContext.read.json("hdfs:///user/s1638696/wx_data/*")
    ac_df = sqlContext.read.json("hdfs:///user/s1638696/flight_data/2017-03*")


def is_area(area_str):
	l = ast.literal_eval(area_str.geometry.coordinates[0])
	if not type(l) == list:
		return False
	if not type(l[0]) == list:
		return False
	return True

sigmets = (df.flatMap(lambda r: r.features)
    .filter(lambda r: r.properties.top is not None and r.properties.base is not None and r.properties.v_from is not None and r.properties.v_to is not None)
    .filter(is_area))

sigmets = sigmets.filter(lambda s: type(ast.literal_eval(s.geometry.coordinates[0])) == list)
wx_blocks = sigmets.flatMap(lambda s: [(block, s) for block in u.get_covered_blocks(ast.literal_eval(s.geometry.coordinates[0]))])
#wx_per_block = wx_blocks.reduceByKey(lambda a, b: a + b)

#wx = dict(wx_per_block.collect())
    


def flightdata_to_tuple(data):
    block_name = u.block_name(data.Lat, data.Long)
    #return (38.894073, 44.247735, 36346, 1513641710343, u'Turkish Airlines')
    #todo: blockname contains day based on PosTime
    return (block_name, (data.Lat, data.Long, data.GAlt, data.PosTime, data.Op))


def inside_poly(d, w):
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

    expr = posTime>w_from and posTime<w_to and w_top > gAlt and w_base < gAlt and (not USE_POLYGON_CHECK or u.inside_polygon(lat, long, ast.literal_eval(w['geometry']['coordinates'][0])))
    return expr

def map_dw(block_name, d, w):
    weather_type = w['properties']['hazard'] or ""
    op = d[4]
    return ((block_name, op), 1)

def groups_to_totals(block_name, items):
    total = sum([v for k,v in items])
    res = [(k, float(v)/total ) for k,v in items]
    return res


import json
# this is going to be very big blocks! introduce time?
#    .reduceByKey(lambda a, b: a+b )
totals = (ac_df.where( (ac_df.PosTime > 0) & (ac_df.Op.isNotNull()) & (ac_df.GAlt>0) & (ac_df.Lat.isNotNull()) & (ac_df.Long.isNotNull()) & (ac_df.Reg.isNotNull()) & (ac_df.Reg != '') & (ac_df.Reg != ac_df.Call) & (ac_df.PosStale.isNull()) & (ac_df.CallSus == False))
    .map(flightdata_to_tuple)
    .join(wx_blocks) 
    .filter(lambda kv: inside_poly(kv[1][0], kv[1][1]))
    .map(lambda kv: map_dw(kv[0], kv[1][0], kv[1][1]))
    .aggregateByKey(0, lambda a,b: a+b, lambda a,b: a+b)
    .groupBy(lambda (k,v): (k[0]))
    .flatMap(lambda (k,v): groups_to_totals(k,v))
    )

#totals.foreach(groups_to_totals)

if USE_FARM:
    totals.collect()
else:
    totals.toDF().write.json("hdfs:///user/s1638696/ac_output/al_given_wx_03", mode="overwrite")
