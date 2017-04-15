# LogAnalysis-ApacheSpark
Log files analysis using Apache Spark. We will focus on Linux syslog files and more precisely on the messages file located in /var/log on most Linux distributions. Such analyses have various applications in system administration, e.g., for security diagnosticso

Used library<br />
Apache Spark 2.1.0<br />
Python 2.7<br />

To run the code follow steps below:
1. set environment variable SPARK_HOME which consist path of spark home directory.
2. append path of spark's bin directory in PATH variable.
3. run ./log_analyzer -q <i> <dir1> <dir2> (Where <dir1> and <dir2> are two directories containing log files collected on two different
hosts, and <i> is the question number and value of i between 1 to 9.)
 
