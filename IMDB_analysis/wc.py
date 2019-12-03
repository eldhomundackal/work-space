from pyspark import SparkContext,SparkConf;


conf= SparkConf().setMaster("local").setAppName("wc")
sc=SparkContext(conf=conf)

Filerdd=sc.textFile("C:\work\work-space\IMDB_analysis\Data\ml-1m\db_test..txt")

splitFrdd=Filerdd.flatMap(lambda line : line.split(" "))
elementmap=splitFrdd.map(lambda ele :(ele ,1))

elesum= elementmap.reduceByKey(lambda a , b : a+b).collect()


for i in elesum:
    print(i)







