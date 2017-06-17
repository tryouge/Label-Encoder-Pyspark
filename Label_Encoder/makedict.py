#Import required dependencies
import json
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline

#Reading the input file into a dataframe
csv_file = raw_input("Enter the path of the file from which a dictionary will be made:\n")
df = sqlContext.read.load(csv_file,
						format='com.databricks.spark.csv',
						header='true',
						inferSchema='true')

#Dropping entries with Null/NA values to avoid errors while encoding
df=df.na.drop()

#Dropping the default index column as it lead to errors while writing and reading again
df=df.drop('_c0')

#Finding the columns that contain categorical data
string_cols=[]
for (a,b) in df.dtypes:
	if b == 'string':
		string_cols.append(a)

#Fitting and Transforming the pipeline of StringIndexers
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(df) for column in string_cols]
pipeline = Pipeline(stages=indexers)
df_r = pipeline.fit(df).transform(df)

#List of names of encoded columns which is 'column_index'
index_cols=[c+"_index" for c in string_cols]
a=len(string_cols)

#Making a dictionary to be saved on to disk
dictlist = [dict() for i in range(a)]
for i in range(a):
	tempdf=df_r.select(string_cols[i], index_cols[i]).distinct()
	keys=tempdf.select(string_cols[i]).rdd.map(lambda x:x[0]).collect()
	values=tempdf.select(index_cols[i]).rdd.map(lambda x:x[0]).collect()
	dictlist[i] = dict(zip(keys, values))

#Printing output
print dictlist
for c in string_cols:
	df_r=df_r.drop(c)

df_r.show()

#Writing dictionary and name of categorical columns into files
with open('dictfile', 'w') as fp:
	json.dump(dictlist, fp)

with open('colsfile', 'w') as fp:
	json.dump(string_cols, fp)