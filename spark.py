import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import os
import sys
import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType, StringType



sc = SparkContext("local[2]", "MLlib_project")
sql_context = SQLContext(sc)
ssc = StreamingContext(sc, 5)



lines = ssc.socketTextStream("localhost", 6100)


words0 = lines.flatMap(lambda line: line.split("feature0"))
print("lines type",type(lines))
print("-------------------------")
print("words type",type(words))
words.pprint()

print("xxxxxxxxxxxxxxxxxxxxxxxxx")
ssc.start()
ssc.awaitTermination()
#query2 = lines.writeStream.format("console").start()

#result = lines.toJSON()
#parsed = json.loads(lines)
#json_sdf = spark.readStream.json(tempfile.mkdtemp(), schema = sdf_schema)
#json_sdf.isStreaming

#lines.printSchema()

#query2.awaitTermination()
# Split the lines into words
#words = lines.select(explode(split(lines.value, ":")).alias("word")).writeStream.format("console").start()

# Generate running word count
#wordCounts = words.groupBy("word").count()




'''import os
import sys
import json
import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType, StringType


#sc = spark.sparkContext
#sc = SparkContext(master=f"local[{os.cpu_count()-1}]", appName="fpl")
sc = SparkContext("local[2]", "MLlib_project")
sql_context = SQLContext(sc)
ssc = StreamingContext(sc, 5)



def getMetrics(rdd):
    #r = result.map( lambda elem: list(elem))
    #r = [json.loads(x) for x in rdd]
    for i in rdd:
        r=i.map(lambda elem: list(elem))


lines = ssc.socketTextStream("localhost", 6101)
#lines.foreachRDD(getMetrics)
x=lines.map(lambda elem: list(elem))
print("--------------------------------------")
print(x)
ssc.start()
ssc.awaitTermination()
ssc.stop()'''



'''import json,csv,os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "MLlib_project")
ssc = StreamingContext(sc, 1)

def getMetrics(rdd):
    r = [json.loads(x) for x in rdd]
    match = r[0]
    events = r[1:]


#ssc.checkpoint("checkpoint")
lines = ssc.socketTextStream("localhost", 6101)
lines.foreachRDD(getMetrics)
print(lines)
ssc.start()
ssc.awaitTermination()
ssc.stop()'''
