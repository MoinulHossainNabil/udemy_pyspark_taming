from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("/home/bjit-542/Big Data/SparkCourse/ml-100k/u.item",encoding = "ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
            #print(movieNames)
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

#nameDict = loadMovieNames()  #for without broadcast
nameDict=sc.broadcast(loadMovieNames())


lines = sc.textFile("/home/bjit-542/Big Data/SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()
#for s in sortedMovies:
 #   print(s)

sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))
#sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict[countMovie[1]], countMovie[0])) #for without broadcast
sortedMoviesWithNames=sortedMoviesWithNames.map(lambda x: (x[1],x[0])).sortByKey(ascending=False)

results = sortedMoviesWithNames.collect()

sortedMoviesWithNames.foreach(lambda r: print(r))
