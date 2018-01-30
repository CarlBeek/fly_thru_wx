

from pyspark import SparkContext
from pyspark.sql import SQLContext

# iterate over blocks
# 
sc = SparkContext(appName= "al_given_wx")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)




probs = sqlContext.read.json("hdfs:///user/s1638696/ac_output/probs_01").rdd

af = (probs
    .filter(lambda (k,v): k[1] == "Air France")
    .map(lambda (k,v): (k[0], v)) # easyJet # Ryanair
    .coalesce(1)
    .collect())

with open("af_alprob_01.json", "w") as fp:
    import json
    json.dump(af, fp)

"""
ej.toDF().write.json("hdfs:///user/s1638696/ac_output/final_easyJet", mode="overwrite")

ry = (probs
    .filter(lambda (k,v): k[1] == "Ryanair")
    .map(lambda (k,v): (k[0], v)) # easyJet # Ryanair
    .coalesce(1))
ry.toDF().write.json("hdfs:///user/s1638696/ac_output/final_Ryanair", mode="overwrite")

"""
