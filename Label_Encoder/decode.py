import json
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, create_map, lit
from itertools import chain

csv_file = raw_input("Enter the path of the file to be encoded:\n")
df = sqlContext.read.load(csv_file,
						format='com.databricks.spark.csv',
						header='true',
						inferSchema='true')

df=df.drop('_c0')
df=df.na.drop()
with open('dictfile', 'r') as fp:
	dictlist=json.load(fp)

with open('colsfile', 'r') as fp:
	string_cols=json.load(fp)

index_cols=[c+'_index' for c in string_cols]
a=len(dictlist)
df_d=df

invdictlist=[dict() for i in range(a)]
for i in range(a):
	invdictlist[i]={v:k for k,v in dictlist[i].iteritems()}

for i in range(a):
	mapping=invdictlist[i]
	mapping_expr=create_map([lit(x) for x in chain(*mapping.items())])
	df_d=df_d.withColumn(string_cols[i], mapping_expr.getItem(col(string_cols[i]+'_index')))

for c in index_cols:
	df_d=df_d.drop(c)

df_d.show()
output_file = csv_file
output_file = output_file.replace('.csv', '_decoded.csv')
df_d.toPandas().to_csv(output_file)

