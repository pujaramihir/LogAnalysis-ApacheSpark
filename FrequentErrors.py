from pyspark import SparkConf, SparkContext
from operator import add
import os
import sys

def removeExtraString(error):
    errorList = error.split()
    errorStr = ""
    for i in range(len(errorList)):
        if i > 3:
            errorStr += str(errorList[i])+ " "
    
    return errorStr
    
configuration = SparkConf().setMaster("local[*]").setAppName("LogAnalysis")
spark = SparkContext(conf=configuration)
spark.setLogLevel('ERROR')

arguments = sys.argv

if len(arguments) == 3:  
    firstHost = spark.textFile(str(os.getcwd()) + "/" + str(arguments[1]) + "/")
    secondHost = spark.textFile(str(os.getcwd()) + "/" + str(arguments[2]) + "/")
    
    firstMapResult = firstHost.filter(lambda l: "error" in l.lower()).map(removeExtraString).map(lambda l:(l,1))
    firstResult = firstMapResult.reduceByKey(add).sortBy(lambda l: l[1],ascending = False).take(5)

    
    print "* Q6: 5 most frequent error messages"            
    print "  + " + str(arguments[1]) + ":"
    for (key,value) in firstResult:
        print "    - ("+str(value)+", '"+str(key)+"')"
            
    
else:
    print "Invalid arguments"
