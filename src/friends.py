from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("Local").setAppName("Friends")
sc=SparkContext.getOrCreate()
def findFriendsByAge(inputData):
    dataset=inputData.split(',')
    age=int(dataset[2])
    no_friends=int(dataset[3])
    return (age,no_friends)
     
filepath="/home/bjit-542/Big Data/Udemy Course/Day 2/fakefriends.csv"
friendsdata=sc.textFile(filepath)
rdd=friendsdata.map(findFriendsByAge)
#Mapping(33 , (385,1)),(33,(2,1)) and then reduceByKey (33,(385+2),(1+1))..
#totalByAges=rdd.mapValues(lambda x : (x , 1)).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))
#resultOfFriendsByAge=totalByAges.collect()
#for r in result:
    #print(r)
totalByAges=rdd.mapValues(lambda x : (x , 1)).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))
averageByAge=totalByAges.mapValues(lambda x : x[0]/x[1])
result=averageByAge.collect()
for r in result:
    print(r)
averageByAge.saveAsTextFile("/home/bjit-542/Big Data/Udemy Course/Day 2/friends.txt")
