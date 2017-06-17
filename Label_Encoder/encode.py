#Import required dependencies
import json
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, create_map, lit
from itertools import chain

#Reading the input file into a dataframe
csv_file = raw_input("Enter the path of the file to be encoded:\n")
df = sqlContext.read.load(csv_file,
						format='com.databricks.spark.csv',
						header='true',
						inferSchema='true')

df=df.drop('_c0')

#Dropping entries with Null/NA values to avoid errors while encoding
df=df.na.drop()

with open('dictfile', 'r') as fp:
	dictlist=json.load(fp)

with open('colsfile', 'r') as fp:
	string_cols=json.load(fp)

a=len(dictlist)
df_t=df

#Encoding the data using dictionary
for i in range(a):
	mapping=dictlist[i]
	mapping_expr=create_map([lit(x) for x in chain(*mapping.items())])
	df_t=df_t.withColumn(string_cols[i]+'_index', mapping_expr.getItem(col(string_cols[i])))

for c in string_cols:
	df_t=df_t.drop(c)

df_t.show()

#Writing output to file
output_file = csv_file
output_file = output_file.replace('.csv', '_encoded.csv')
df_t.toPandas().to_csv(output_file)
