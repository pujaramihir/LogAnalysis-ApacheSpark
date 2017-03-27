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
    firstResult = firstMapResult.reduceByKey(add).sortByKey(ascending = True).collect()
    
    secondMapResult = secondHost.map(lambda l:l.split(":")).map(matchToUser).map(lambda l: (l, 1))
    secondResult = secondMapResult.reduceByKey(add).sortByKey(ascending = True).collect()
    
    i = 0 
    firstHostUsers = []
    
    for (user,count) in firstResult:
        if user != 'False':
            user = str(user)
            firstHostUsers.append((user[:-1],"user-"+str(i)))
            i = i + 1

    i = 0
    secondHostUsers = []
    
    for (user,count) in secondResult:
        if user != 'False':
            user = str(user)
            secondHostUsers.append((user[:-1],"user-"+str(i)))
            i = i + 1            
            
    print firstHostUsers
    print secondHostUsers
    #print secondResult
    
    """
    print "* Q8: users who started a session on exactly one host, with host name."
    print "  + : " + str(users)
    """    
else:
    print "Invalid arguments"
