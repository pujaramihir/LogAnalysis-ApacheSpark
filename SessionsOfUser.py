from pyspark import SparkConf,SparkContext
from operator import add
import os
import sys
import re

def matchToUser(logString):
    p = re.compile('( Starting Session [0-9]+ of user achille.)+')
    m = p.match(logString[3])
    if m:
        return "True"
    else:
        return "False"

conffiguration = SparkConf().setMaster("local[*]").setAppName("LogAnalysis")
spark = SparkContext(conf = conffiguration)
spark.setLogLevel('ERROR')

arguments = sys.argv

if len(arguments) == 3:  
    firstHost = spark.textFile(str(os.getcwd())+"/"+str(arguments[1])+"/")
    secondHost = spark.textFile(str(os.getcwd())+"/"+str(arguments[2])+"/")
    firstMapResult = firstHost.map(lambda l:l.split(":")).map(matchToUser).map(lambda l: (l,1))
    firstResult = firstMapResult.reduceByKey(add).collect()
    
    secondMapResult = secondHost.map(lambda l:l.split(":")).map(matchToUser).map(lambda l: (l,1))
    secondResult = secondMapResult.reduceByKey(add).collect()
    
    
    print "* Q2: sessions of user 'achille'"
    for (key,value) in firstResult:
        if key == 'True':
            print "  + "+str(arguments[1])+": "+str(value)
    
    for (key,value) in secondResult:
        if key == 'True':
            print "  + "+str(arguments[2])+": "+str(value)
    
    
else:
    print "Invalid arguments"