from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").set("spark.executor.instances", "4").setAppName("imdb_analysis")
sc = SparkContext(conf=conf)
def rating():

## reading data
    radata = os.path.join("C:\work\work-space\IMDB_analysis\Data", "ml-1m", "ratings.dat")
    ratingdata=sc.textFile(radata)
    racollect = ratingdata.map(lambda a : a.split("::")[1])
    racollect2 = racollect.map(lambda a : (a, 1)).reduceByKey(lambda a,b : a+b)

    return racollect2

def movie():

  movpath = os.path.join("C:\work\work-space\IMDB_analysis\Data", "ml-1m", "movies.dat")
  movdata = sc.textFile(movpath)
  movname = movdata.map(lambda a : a.split("::")[:2])
  # print(movname.take(10))
# rating()
  return movname


def user():
    user_path = os.path.join("C:\work\work-space\IMDB_analysis\Data", "ml-1m", "users.dat")
    user_rdd = sc.textFile(user_path)
    print(user_rdd.take(5))
# def process ():
#     rating_rdd = rating()
#     movie_rdd=movie()
#     print(rating_rdd.take(5))
#     # print(movie_rdd.take(5))
#     m_watch_mov = rating_rdd.join(movie_rdd).map(lambda x: (x[1][1],x[1][0])).sortBy(lambda a : a[1],ascending=False).collect()
#     for mov in m_watch_mov:
#         print(mov)
# process()
user()