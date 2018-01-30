
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName= "al_given_wx")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)

df_al_wx = sqlContext.read.json("/user/s1638696/ac_output/al_given_wx_01").rdd.groupBy(lambda (k,v): k[0])
df_wx = sqlContext.read.json("/user/s1638696/ac_output/wx_prob.json").rdd
df_al = sqlContext.read.json("/user/s1638696/ac_output/al_prob.json").rdd.groupBy(lambda (k,v): k[0])
df_b = sqlContext.read.json("/user/s1638696/ac_output/b_given_al_prob_januari.json").rdd.groupBy(lambda (k,v): k[0]) #(block, op, prob) groupBy(op)

def block_prob(block_name, al_wx, al, wx, bList):
    try:
        al_dict = {}
        for (k,v) in al:
            op = k[1]
            al_dict[op] = v
        b = {}
        for (k,v) in bList:
            op = k[1]
            b[op] = v
        probs_dict = {}
        for (k,v) in al_wx:
            op = k[1]
            probs_dict[op] = b[op]*(float(v) * wx / al_dict[op])
        return [((block_name, op), p) for (op, p) in probs_dict.items()]
    except Exception, e:
        print type(block_name), type(al_wx), type(al), type(wx), type(bList)
        print "Exception in blockprob"
    #total = sum(list(probs_dict.values()))
    #return [((block_name, op), p/total) for (op,p) in probs_dict.items()]



df_probs = (df_al_wx
    .join(df_al)
    .join(df_wx)
    .join(df_b)
    .flatMap(lambda kv: block_prob(kv[0], kv[1][0][0][0][0], kv[1][0][0][1][0],kv[1][0][1], kv[1][1][0]))
    .toDF().write.json("hdfs:///user/s1638696/ac_output/probs_01", mode="overwrite"))

(df_probs.
    .groupBy(lambda (k,v): k[1]) # groupBy airline
    .map(lambda kv: (kv[0], sum([v for (k,v) in kv[1]])))
    .coalesce(1)
    .toDF().write.json("hdfs:///user/s1638696/ac_output/final_probabilities_01", mode="overwrite"))
"""
    .toDF().write.json("hdfs:///user/s1638696/ac_output/join_01")
)
"""