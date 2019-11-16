# from pyspark import SparkConf, SparkContext, SparkFiles
import requests
import time
# from pyspark.sql.types import *
# from pyspark.sql import SparkSession

# URL = "https://speedfs.s3.us-east-2.amazonaws.com/nameFile2"
URL_fil2 = "http://10.192.148.161:8081/nameFile2"

response = requests.get(url=URL_fil2, stream=True)

with open("nameFile","w") as nameFile:
    nameFile.write(response.content)
nameFile.close()
print (reqeusts.content)

start = time.time()

count = 0

for line in response.iter_content(chunk_size = 8092):
    count+=1
print ("Load Time: ", time.time()-start, " Count: ", count)


conf = SparkConf().setMaster("local[1]").setAppName("countNames")
conf.set("spark.driver.bindAddress", "127.0.0.1")
conf.set("spark.executor.memory", "14g")
conf.set("spark.driver.memory", "12g")
sc = SparkContext(conf = conf)

sc.addFile(URL)

def loadData():
    corpus = []
    with open(SparkFiles.get("nameFile2")) as testFile:
        for line in testFile:
            corpus.append(line.strip())
        print (corpus[:4])
    return corpus

start = time.time()
corpus = loadData()

print("File Loading time: ", time.time()-start)

#computationTime
computationTimeStart = time.time()
rdd = sc.parallelize(corpus)

name_field = StructField("name", StringType(), True)

schema = StructType([name_field])
dataFrames = SparkSession.createDataFrame(rdd, schema)
dataFrames.createOrReplaceTempView("nameData")

spark.sql("select * from nameData where name ='rahul'").count()
spark.sql("select * from nameData where name ='ani'").count()

# print "textData got cached > %s" % (textData.persist().is_cached )

# numRahuls = textData.filter(lambda s: 'rahul' in s).count()
# numAnis = textData.filter(lambda s: 'ani' in s).count()
print "Lines with rahul: %i, lines with ani: %i" % (numRahuls, numAnis)
print "Time elapsed: ", time.time()-computationTimeStart