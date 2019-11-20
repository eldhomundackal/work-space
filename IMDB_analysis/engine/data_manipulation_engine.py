from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("imdb_analysis")
sc = SparkContext(conf=conf)

def moveidat():
    moviedata = os.path.join("C:\work\work-space\IMDB_analysis\Data", "ml-1m", "movies.dat")
    movierdd = sc.textFile(moviedata)
    moviedata_heading_rdd = sc.parallelize(["Movie_id::Title::Genres"]).union(movierdd)
    newmovierdd=moviedata_heading_rdd.map(lambda a: a.split("::"))

def rating():

    ratingdata = os.path.join("C:\work\work-space\IMDB_analysis\Data", "ml-1m", "ratings.dat")
    ratingrdd=sc.textFile(ratingdata)

    # ratingdata_heading_rdd = sc.parallelize(["Userid::MovieId::rating::Timestamp"])
    # ratingread_rdd =  ratingrdd.map(lambda a: a.split("::"))

    # rating= ratingrdd.map(lambda x: x.split()[2])
    # print(rating.countByValue())

def user():
    userdata=os.path.join("C:\work\work-space\IMDB_analysis\Data", "ml-1m", "users.dat")
    userrdd =sc.textFile(userdata)
    userhead =sc.parallelize(["userId::gender::Age::Occupation::Ziocode"])
    usernewrdd= userhead.union(userrdd)
    userread_rdd =usernewrdd.map(lambda a: a.split("::"))
    # print(userread_rdd.take(10))
    # userread_rdd.saveAsTextFile("C:\work\work-space\IMDB_analysis\Data\ml-1m\out1.txt")
# moveidat()
rating()
# user()