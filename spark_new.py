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
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql import SparkSession

schema = schema = StructType([
    StructField("feature0", IntegerType(), False),
    StructField("feature1", StringType(), False)])

#df.printSchema()

sc = SparkContext("local[2]", "MLlib_project")
sql_context = SQLContext(sc)
ssc = StreamingContext(sc, 5)
spark = SparkSession.builder.appName('MLlib').getOrCreate()

def fun(rdd):
    rdd=rdd.collect()
    if rdd:
        #print(rdd)
        print("******************************************************************")
        json_rdd_dict=json.loads(rdd[0])
        #print(json_rdd_dict)
        df = spark.createDataFrame(sc.emptyRDD(),schema)
        for key, value in json_rdd_dict.items() :

            ''' #print("{}\t{}".format(key,value))
            print(val)
            dff = spark.read.json(sc.parallelize([val]))'''
            val=value
            rdd = spark.sparkContext.parallelize([Row(feature0=int(val['feature0']), feature1=val['feature1'])])
            dff = spark.createDataFrame(rdd, schema)
            #dff.show()

            df = df.unionAll(dff)
            print(key,"#####################################")
            #df.show()
        #print("ended")
        df.show()
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")


        '''newJson = '{"Name":"something","Url":"https://stackoverflow.com","Author":"jangcy","BlogEntries":100,"Caller":"jangcy"}'

        df.show(truncate=False)'''




lines = ssc.socketTextStream("localhost", 6100)
words = lines.flatMap(lambda line: line.split("\n"))
print("---------------------------------------------------------------")
#print(words)
words.foreachRDD(fun)


print("xxxxxxxxxxxxxxxxxxxxxxxxx")
ssc.start()
ssc.awaitTermination()
