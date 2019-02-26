from pyspark import SparkConf,SparkContext
import re
conf=SparkConf().setMaster("local").setAppName("wordcount")
sc=SparkContext.getOrCreate()
def parseLine(file):
    #return re.compile(r'W+',re.UNICODE).split(file.upper())
    return file.split(" ")
def mapreduce(data):
    return (data,1)
file=sc.textFile("/home/bjit-542/Big Data/SparkCourse/Book.txt")
lines=file.flatMap(parseLine)
result=lines.countByValue()
for k,v in result.items():
    print(k,v)
