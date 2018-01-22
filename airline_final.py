
from pyspark import SparkContext
from pyspark.sql import SQLContext

# iterate over blocks
# 
sc = SparkContext(appName= "al_given_wx")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)


df_al_wx = sqlContext.read.json("/user/s1638696/ac_output/al_given_wx_march").rdd.groupBy(lambda (k,v): k[0])
df_wx = sqlContext.read.json("/user/s1638696/ac_output/wx_prob.json").rdd
df_al = sqlContext.read.json("/user/s1638696/ac_output/al_prob.json").rdd.groupBy(lambda (k,v): k[0])


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

(df_al_wx
    .join(df_al)
    .join(df_wx)
    .toDF().write.json("hdfs:///user/s1638696/ac_output/join_per_block", mode="overwrite")
)

#    .flatMap(lambda kv: block_prob(kv[0], kv[1][0][0][0], kv[1][0][1][0],kv[1][1]))
#    .coalesce(1)
#)
#
#probabilities.toDF().write.json("hdfs:///user/s1638696/ac_output/final_per_block", mode="overwrite")






#probabilities.groupBy(lambda (k,v): k[1]).map(lambda (k,v): (k,sum([p[1]/259200 for p in v])))
