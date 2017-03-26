from pyspark import SparkConf, SparkContext
from operator import add
import os
import sys
import re
from pyspark.sql.functions import first

def matchToUser(logString):
    p = re.compile('( Starting Session [0-9a-zA-Z]+ of user [a-zA-z0-9]+.)+')
    m = p.match(logString[3])
    if m:
        user = logString[3].split()
        return user[5]
    else:
        return "False"

conffiguration = SparkConf().setMaster("local[*]").setAppName("LogAnalysis")
spark = SparkContext(conf=conffiguration)
spark.setLogLevel('ERROR')

arguments = sys.argv

if len(arguments) == 3:  
    firstHost = spark.textFile(str(os.getcwd()) + "/" + str(arguments[1]) + "/")
    secondHost = spark.textFile(str(os.getcwd()) + "/" + str(arguments[2]) + "/")
    
    firstMapResult = firstHost.map(lambda l:l.split(":")).map(matchToUser).map(lambda l: (l, 1))
    firstResult = firstMapResult.reduceByKey(add).collect()
    
    secondMapResult = secondHost.map(lambda l:l.split(":")).map(matchToUser).map(lambda l: (l, 1))
    secondResult = secondMapResult.reduceByKey(add).collect()
    
    print "* Q4: sessions per user"
    firstList=[]
    for (key, value) in firstResult:
        if key != 'False':
            firstList.append((str(key)[:-1],value))
            
    print "  + " + str(arguments[1]) + ": " + str(firstList)
    
    secondList=[]
    for (key, value) in secondResult:
        if key != 'False':
            secondList.append((str(key)[:-1],value))
    
    print "  + " + str(arguments[2]) + ": " + str(secondList)
    
else:
    print "Invalid arguments"
