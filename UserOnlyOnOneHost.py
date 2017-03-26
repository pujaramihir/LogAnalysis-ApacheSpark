from pyspark import SparkConf, SparkContext
from operator import add
import os
import sys
import re

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
    
    
    firstList = []
    for (key, value) in firstResult:
        if key != 'False':
            firstList.append(str(key)[:-1])
        
    secondList = []
    for (key, value) in secondResult:
        if key != 'False':
            secondList.append(str(key)[:-1])
    
    commonUsers = list(set(firstList).intersection(secondList))
    
    users = []
    
    for firstUser in firstList:
        if firstUser not in commonUsers:
            users.append((firstUser,str(arguments[1])))

    for secondUser in secondList:
        if secondUser not in commonUsers:
            users.append((secondUser,str(arguments[2])))
            
    print "* Q8: users who started a session on exactly one host, with host name."
    print "  + : " + str(users)    
else:
    print "Invalid arguments"
