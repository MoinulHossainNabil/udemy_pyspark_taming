
# Here is my code snippet for finding total amount of spent by each unique customerId
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrder")
sc = SparkContext.getOrCreate()

def customerDataSet(file):
    lines = file.split(",")
    customerId = int(lines[0])
    productPrice = float(lines[2])
    return (customerId, productPrice)

input = sc.textFile("/home/bjit-542/Big Data/SparkCourse/customer-orders.csv")
customerIdPrice = input.map(customerDataSet)
result = customerIdPrice.reduceByKey(lambda x, y: x+y)
sortedBySpentAmount = result.map(lambda x: (x[1], x[0])).sortByKey().collect()

for s in sortedBySpentAmount:
    print(s)


