

from pyspark import SparkContext
from pyspark.sql import SQLContext

# iterate over blocks
# 
sc = SparkContext(appName= "al_given_wx")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)

def block_prob(block_name, al_wx, al, wx):
    al_dict = {}
    for (k,v) in al:
        op = k[1]
        al_dict[op] = v
    probs_dict = {}
    for (k,v) in al_wx:
        op = k[1]
        probs_dict[op] = float(v) * wx / al_dict[op]
    total = sum(list(probs_dict.values()))
    return [((block_name, op), p/total) for (op,p) in probs_dict.items()]


probs = (sqlContext.read.json("/user/s1638696/ac_output/join_per_block").rdd
    .flatMap(lambda kv: block_prob(kv[0], kv[1][0][0][0], kv[1][0][1][0],kv[1][1]))
    .coalesce(1))


ej = (probs
    .filter(lambda (k,v): k[1] == "easyJet")
    .map(lambda (k,v): (k[0], v)) # easyJet # Ryanair
    .coalesce(1))
ej.toDF().write.json("hdfs:///user/s1638696/ac_output/final_easyJet", mode="overwrite")

ry = (probs
    .filter(lambda (k,v): k[1] == "Ryanair")
    .map(lambda (k,v): (k[0], v)) # easyJet # Ryanair
    .coalesce(1))
ry.toDF().write.json("hdfs:///user/s1638696/ac_output/final_Ryanair", mode="overwrite")


