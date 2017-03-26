from pyspark import SparkConf, SparkContext
from operator import add
import os
import sys

conffiguration = SparkConf().setMaster("local[*]").setAppName("LogAnalysis")
spark = SparkContext(conf=conffiguration)
spark.setLogLevel('ERROR')

arguments = sys.argv

if len(arguments) == 3:  
    firstHost = spark.textFile(str(os.getcwd()) + "/" + str(arguments[1]) + "/")
    secondHost = spark.textFile(str(os.getcwd()) + "/" + str(arguments[2]) + "/")
    
    firstMapResult = firstHost.filter(lambda l: "error" in l.lower())
    secondMapResult = secondHost.filter(lambda l: "error" in l.lower())

    print "* Q5: number of errors"            
    print "  + " + str(arguments[1]) + ": " + str(firstMapResult.count())
    print "  + " + str(arguments[2]) + ": " + str(secondMapResult.count())
else:
    print "Invalid arguments"
