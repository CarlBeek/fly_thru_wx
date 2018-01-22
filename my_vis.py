from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="al_prob_max")
sc.setLogLevel('ERROR')
sqlContext = SQLContext(sc)

ac_df = sqlContext.read.json("hdfs:///user/s1638696/ac_output/al_prob.json")
#al_prob.toDF().write.json("/user/s1638696/ac_output/al_prob.json", mode="overwrite")

def get_max_al(ac_1, ac_2):
    if ac_1[1] > ac_2[1]:
        return ac_1
    else:
        return ac_2

df1 = ac_df.map(lambda ((b,op),p): (b, (op,p))).reduceByKey(get_max_al)
df1.toDF().write.json('hdfs:///user/s1638696/ac_output/max_al.json', mode = 'overwrite')
