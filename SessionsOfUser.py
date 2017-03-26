from pyspark import SparkConf,SparkContext
import os
import sys

def matchToUser(logString):
    return logString[1]

conffiguration = SparkConf().setMaster("local[*]").setAppName("LogAnalysis")
spark = SparkContext(conf = conffiguration)
spark.setLogLevel('ERROR')

arguments = sys.argv

if len(arguments) == 3:  
    firstHost = spark.textFile(str(os.getcwd())+"/"+str(arguments[1])+"/")
    secondHost = spark.textFile(str(os.getcwd())+"/"+str(arguments[2])+"/")
    firstResult = firstHost.map(lambda l:l.split(":")).map()
    
    print firstResult.collect()
else:
    print "Invalid arguments"