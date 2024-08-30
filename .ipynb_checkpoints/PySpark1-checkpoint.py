# -*- coding: utf-8 -*-
"""
Created on Thu Aug 29 10:47:51 2024

@author: Ankit19.Gupta
"""

# 1. Spark Stack: 
    
#     1. Spark Core - Underlying engine for task scheduling like 
#     RR, FCFS etc, memory mgmt, fault recovery etc. iyt has RDDs
#     2. Spark SQL - for querying structured and unstructured data using SQL commands
#     3. Spark Streaming - for livestream real data and process using batches
#     4. MLLib - For ML algorithms and data preprocessing
#     5. GraphX - For implementing graph algorithms and its computation.
#     It can be used for disaster/fraud detection system, page ranking etc
    
    
     
# 2. Spark can be used with Python,R, Java, Scala etc

# 3. Spark Cluster Mgmt Tools: Hadoop Yarn, Apache Mesos, Spark Scheduler

# 4. Storage tools: HDFS (Hadoop File System), Standalone Node, Cloud, RDBMS/NoSQL

# 5. Spark Layered Architecture:

# we need spark to understand data. So, our driver program (or drivers ) consists of 
# "SparkContext" which is used to create RDDs. 
# For every single instance of spark running or spark core running, 
# we need one SparkContext. So, one SparkContext for every single program. 

# 6. This driver program is connected with "Cluster Management" tools 
#    which is required to pull data from various nodes.

# 7. Now this Cluster Manager is connected with "Worker Node" which
#   are the endpoint of our spark architecure where data
#   transformed into information where we getting the task done. 
#   It is having "Executor" for Cache and Task in various working nodes. 

# 8. A driver program runs on the master node of the Spark cluster  
#    It schedules the job execution. It also translates RDDs 
#    into the execution graphs. The driver program can split graphs 
#    into multiple stages	

# 9. The executor is a distributed agent responsible for 
#    the execution of tasks.Every Spark application has its 
#    own executor process. It interacts with storage systems
   
# 10. Cluster is used to deploy spark application. Spark preconfigured 
#     for YARN
  
# 11. YARN controls the resource management, scheduling, and 
#     security when we run Spark apllications on it.

# 12. It is possible to run an application in any mode, 
#     whether it is cluster mode (many nodes) or client mode
#     (one single entiny). 
    
# 13. The spark driver runs inside an ApplicationMaster(AM) process 
#     which is managed by YARN. A single process in a YARN container 
#     is responsible for driving the application and requesting 
#     resources from YARN.

# 14. Every sparkcontext launches a web UI
#     A web ui includes: jobs, stages, storage, environment, executors.
    
# 15. The "Jobs" tab in a web ui shows the status of all spark jobs 
#     in a spark application (i.e. a sparkcontext)    
   
# 16. PySpark shell links the Python API to the Spark Core and 
#     initializes the SparkContext
  
# 17. Many times our code depends on other projects or source codes, 
#     so we need to package them alongside our application to distribute 
#     the code to a spark cluster

#     For this, we create an assembly jar containing our code and 
#     its dependencies Both SBT and Maven have assembly plugins

#     When creating assembly jars, list Spark and Hadoop as provided dependencies

#     Once we have an aseembled jar, we can call the bin/spark-submit script 
#     while passing our jar.The spark-submit script is used to launch application 
#     on the cluster
    
# 18. The following steps will help us to build our first application in spark using python:

#     -- Create a document named PySparkjob.py on the local drive

#     -- open PySparkjob.py in notepad and write: print("hello! this is pyspark") [source code]

#     -- We can upload the PySparkjob.py file on our CloudLab's local storage if we are not using a VM and perform the following steps:

#     check current directory where we have the above file using ls

#     run: spark-submit --master yarn --deploy-mode client PySparkjob.py

#     it will show the output.

###############################################################################

################################ Code #########################################     

# PySpark Job

from pyspark import SparkConf
from pyspark import SparkContext
import numpy as np

conf = SparkConf()
conf.setMaster('local')
conf.setAppName('Spark-Basic')
sc = SparkContext(conf=conf)

def mod(x):
    return (x,np.mod(x,2))

rdd = sc.parallelize(range(1000)).map(mod).take(10)
print(rdd)




