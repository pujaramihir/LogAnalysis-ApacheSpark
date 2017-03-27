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
    
    
def replaceFirstHostUsersToAnonymizeUser(data):
    for (user,anonymizeUser) in firstHostUsers:
        data = str(data.encode('ascii', 'ignore'))
        data = data.replace(user, anonymizeUser)
        
    return data
    
def replaceSecondHostUsersToAnonymizeUser(data):
    for (user,anonymizeUser) in secondHostUsers:
        data = str(data.encode('ascii', 'ignore'))
        data = data.replace(user, anonymizeUser)
        
    return data
    

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
            
    anonymizeFirstHost = firstHost.map(replaceFirstHostUsersToAnonymizeUser)
    anonymizeSecondHost = secondHost.map(replaceSecondHostUsersToAnonymizeUser)
    
    no1 = 10
    while True:
        if not os.path.isdir(os.getcwd() + "/" + str(arguments[1]) + "-anonymized-"+str(no1)+"/"):
            break
        else:
            no1 = no1 + 1
            
    no2 = 10
    while True:
        if not os.path.isdir(os.getcwd() + "/" + str(arguments[2]) + "-anonymized-"+str(no2)+"/"):
            break
        else:
            no2 = no2 + 1
                
    anonymizeFirstHost.saveAsTextFile(os.getcwd() + "/" + str(arguments[1]) + "-anonymized-"+str(no1)+"/")
    anonymizeSecondHost.saveAsTextFile(os.getcwd() + "/" + str(arguments[2]) + "-anonymized-"+str(no2)+"/")
    
    print "  + " + str(arguments[1])
    print "  . User name mapping: " + str(firstHostUsers)
    print "  . Anonymized files: " + str(arguments[1]) + "-anonymized-"+str(no1)
    
    print "  + " + str(arguments[2])
    print "  . User name mapping: " + str(secondHostUsers)
    print "  . Anonymized files: " + str(arguments[2]) + "-anonymized-"+str(no2)
    
else:
    print "Invalid arguments"
