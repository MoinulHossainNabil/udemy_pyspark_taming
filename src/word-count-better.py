import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text)

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext.getOrCreate()

input = sc.textFile("/home/bjit-542/Big Data/SparkCourse/Book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
