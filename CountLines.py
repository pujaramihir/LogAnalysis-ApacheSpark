from pyspark import SparkConf,SparkContext
import os
import sys

conffiguration = SparkConf().setMaster("local[*]").setAppName("LogAnalysis")
spark = SparkContext(conf = conffiguration)
spark.setLogLevel('ERROR')

arguments = sys.argv

if len(arguments) == 3:  
    firstHost = spark.textFile(str(os.getcwd())+"/"+str(arguments[1])+"/")
    secondHost = spark.textFile(str(os.getcwd())+"/"+str(arguments[2])+"/")
    print "* Q1: line counts"
    print "  + "+str(arguments[1])+": "+str(firstHost.count())
    print "  + "+str(arguments[2])+": "+str(secondHost.count())
else:
    print "Invalid arguments"