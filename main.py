# This is a sample Python script.
import os
from graphframes import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    return spark


def toCSVLineRDD(rdd):
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    if isinstance(data, RDD):
        if data.count() > 0:
            return toCSVLineRDD(data)
        else:
            return ""
    elif isinstance(data, DataFrame):
        if data.count() > 0:
            return toCSVLineRDD(data.rdd)
        else:
            return ""
    return None

def prepareQuery(q):
    motif_finder = udf(lambda src,dst: "(a" + str(src) + ")-[e" + str(src) + str(dst) + "]->(a" + str(dst) + ")")
    q = q.withColumn("motif", motif_finder(q.src, q.dst))

    return q

def search(g, q):
    motif_str = q.select("motif")\
        .rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
        .reduce(lambda x,y: ";".join([x,y]))

    motif = g.find(motif_str)

    edges = q.select("src", "dst", "src_name", "dst_name").rdd.map(lambda r: r.asDict()).collect()

    filter_values = []

    for edge in edges:
        src = "a" + edge['src'] + ".name =='" + edge['src_name'] + "'"
        filter_values.append(src)

        dst = "a" + edge['dst'] + ".name =='" + edge['dst_name'] + "'"
        filter_values.append(dst)

    filter_values = list(set(filter_values))

    for filter in filter_values:
        motif = motif.filter(filter)

    print("Motif Info: " + str(q.select("query_id").distinct().collect()[0]))
    print(toCSVLineRDD(motif.rdd))
    print(motif.count())

if __name__ == '__main__':
    init_spark()
    spark = SparkSession._create_shell_session()
    v = spark.read.csv("vertex.txt", header=True, mode="DROPMALFORMED")
    e = spark.read.csv("edge.txt", header=True, mode="DROPMALFORMED")
    q = spark.read.csv("query.txt", header=True, mode="DROPMALFORMED")

    g = GraphFrame(v, e)
    q = prepareQuery(q)

    min = int(q.select("query_id").rdd.min()[0])
    max = int(q.select("query_id").rdd.max()[0]) + 1

    for i in range(min, max):
        filtered_query = q.where(q.query_id == i)
        search(g, filtered_query)