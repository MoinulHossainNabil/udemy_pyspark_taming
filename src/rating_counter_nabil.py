from pyspark import SparkConf as sconf, SparkContext as scont
import collections
def ratingssplit(rating):
    return (rating.split()[2])

conf = sconf().setMaster("local").setAppName("RatingsHistogram")
sc = scont.getOrCreate()

lines = sc.textFile("./ml-100k/u.data")
ratings = lines.map(ratingssplit)
result = ratings.countByValue()
print("CountByValue is ",result)
print((sorted(result.items())))
sortedResults = collections.OrderedDict(sorted(result.items(),reverse=True))
print(sortedResults)
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

