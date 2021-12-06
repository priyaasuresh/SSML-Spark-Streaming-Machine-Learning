import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import os
import sys
import csv
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
import pickle
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.evaluation import BinaryClassificationMetrics


def clean_tweet(row):
	split_row=row.split(",")
	sentiment=split_row[1]
	tweet=split_row[3]
	tweet.strip()
	tweet=re.sub(r"http\S+", "", tweet)
	tweet=re.sub("[^a-zA-z]", " ", tweet)
	tweet=re.sub(r"@\w+", "", tweet)
	tweet=re.sub(r"#", "", tweet)
	tweet=re.sub(r"RT", "", tweet)
	tweet=re.sub(r":", "", tweet)
	tweet=tweet.lower()
	#convert tweet in to list of words
	words_list=tweet.split()

schema = schema = StructType([
	StructField("sentiment", IntegerType(), False),
	StructField("tweet", StringType(), False)])

#df.printSchema()

sc = SparkContext("local[2]", "MLlib_project")
sql_context = SQLContext(sc)
ssc = StreamingContext(sc, 5)
spark = SparkSession.builder.appName('MLlib').getOrCreate()

def fun(rdd):
	global spark,schema
	rec=rdd.collect()
	dicts=[i for j in rec for i in list(json.loads(j).values())]
	if len(dicts)==0:
		return
	df=spark.createDataFrame([Row(feature0=d['feature0'],feature1=clean_tweet(d['feature1'])) for d in dicts],schema=['feature0','feature1'])
	df.select("feature1").show()
	from pyspark.ml.feature import StopWordsRemover
	remover=StopWordsRemover(inputCol="tweet", outputCol="filtered")
	filtered_df=remover.transform(df)
    
	#now make 2-gram model
	from pyspark.ml.feature import NGram

	ngram=NGram(n=2, inputCol="filtered", outputCol="2gram")
	gram_df=ngram.transform(filtered_df)
    
	#now make term frequency vectors out of data frame to feed machine
	from pyspark.ml.feature import HashingTF,IDF
	hashingtf=HashingTF(inputCol="2gram", outputCol="tf", numFeatures=20000)
	tf_df=hashingtf.transform(gram_df)
    
	#tf-idf
	idfModel=idf.fit(tf_df)
	idf_df=idfModel.transform(tf_df)
	#convert dataframe t rdd, to make a LabeledPoint tuple(label, feature, vector) for machine
	tf_rdd=tf_df.rdd
	from pyspark.mllib.regression import LabeledPoint
	from pyspark.mllib.linalg import  Vectors as MLLibVectors
	train_dataset=tf_rdd.map(lambda x: LabeledPoint(float(x.sentiment), MLLibVectors.fromML(x.tf)))
	#split dataset into train, test
	train, test=train_dataset.randomSplit([0.8, 0.2])
	print(train.first())
	print(test.first())
	#create Model
	#now train and save the model
	from pyspark.mllib.classification import NaiveBayes
	import shutil
	#training
	print("************************TRAINIG*******************************")
	model=NaiveBayes.train(train, 1.0)
	filename = 'finalized_model.sav'
	pickle.dump(model, open(filename, 'wb'))
	print("*****************************TRAINING COMPLETE************************************")
	print("************************TESTING***********************************")
	# load the model from disk
	loaded_model = pickle.load(open(filename, 'rb'))
	predictionAndLabel=test.map(lambda x: (x.label, loaded_model.predict(x.features)))
	accuracy=1.0*predictionAndLabel.filter(lambda x: x[0]==x[1]).count()/test.count()
	print("Model Accuracy is ", accuracy)
	metrics = BinaryClassificationMetrics(predictionAndLabel)
	# Area under precision-recall curve
	#print("Area under PR = %s" % metrics.areaUnderPR)
	# Area under ROC curve
	#print("Area under ROC = %s" % metrics.areaUnderROC)
	metrics = MulticlassMetrics(predictionAndLabels)

	# Overall statistics
	#precision = metrics.precision()
	#recall = metrics.recall()
	#f1Score = metrics.fMeasure()
	#print("Summary Stats")
	#print("Precision = %s" % precision)
	#print("Recall = %s" % recall)
	#print("F1 Score = %s" % f1Score)
	print("*****************TESTING COMPLETED*****************************")




lines = ssc.socketTextStream("localhost", 6100)
words = lines.flatMap(lambda line: line.split("\n"))
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
words.foreachRDD(fun)
print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
ssc.start()
ssc.awaitTermination()






