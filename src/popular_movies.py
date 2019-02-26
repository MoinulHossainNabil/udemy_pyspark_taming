from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("Popular Movies")
sc=SparkContext.getOrCreate()
def movies(m):
    print(m)
    return (int(m.split()[1],1))
moviesAsText=sc.textFile("/home/bjit-542/Big Data/SparkCourse/ml-100k/u.data")

movieListById=moviesAsText.map(movies)
movieListById.forEach(lambda x:print(x))
resultSet=movieListById.reduceByKey(lambda x,y: x+y)
flipped=resultSet.map(lambda x,y: (y,x))
#sortMovies=flipped.sortByKey()
#result=resultSet.collect()
#result.forEach(lambda x:print(x))