from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").set("spark.executor.instances", "4").setAppName("imdb_analysis")
sc = SparkContext(conf=conf)
def rating():
## reading data
    ratingdata = os.path.join("C:\work\work-space\IMDB_analysis\Data", "ml-1m", "ratings.dat")
    ratingdata=sc.textFile(ratingdata)
    ## perform transformation on dataset

    racollect = ratingdata.map(lambda a : a.split("::")[2])
    racollect2 = racollect.map(lambda a : (a,1)).reduceByKey(lambda a,b : a+b).sortBy(lambda a : a[1],ascending=False)
    ## Create an Rdd for header
    head = sc.parallelize(["MovieId,No.views"])
    print(head.take(1))
    totalview= head.union(racollect2)
    print(totalview.getNumPartitions())
## save result to a file as single partition using coalesce
    totalview.coalesce(1).saveAsTextFile("C:\work\work-space\IMDB_analysis\Data\out121")
rating()


