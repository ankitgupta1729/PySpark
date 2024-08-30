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

# RDD stands for resilient distributed dataset. RDDs are the fundamental structure of spark. 
# Every dataset in RDD is divided into various local partitions which is needed for different 
# clusters.

# creating spark session

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.stop() # stopping the session

# creating RDD using list
conf=SparkConf()
conf.setMaster('local')
conf.setAppName('Spark-Basic1')
sc = SparkContext(conf=conf)
values=[1,2,3,4,5]
rdd= sc.parallelize(values).take(3)
print(rdd)

# RDDs are immutable and structured partitions of data

# Spark RDDs supports:

# A. In-Memory Computation
# B. Lazy Evaluation
# C. Fault Tolerance
# D. Immutability
# E. Partitioning
# F. Persistence
# G. Coarse Grained Operation

# RDDs are the main logical data unit in spark.

# They are a distributed collection of objects, stored in memory or 
# on disks of different machines of a cluster

# A single RDD can be divided into multiple logical partitions so that these partitions can be stored and 
# processed on different machines of a cluster

# RDDs in spark can be cached and used again for future transformations

# They are lazily evaluated i.e. spark delays the RDD evaluation until it is really needed

# RDDs are arrays or structured data and this structured data is converted into DAG(Directed acyclic graph)
#    so that data can be flow from one node to another in structured manner. This is the work of DAGScheduler.
# C. Once DAGScheduler finished its job where it assembeled, ordered and stored it. Now, it passed it into Cluster Manger.
#    and it pulls the data from all places and each operation is considered a task which is done by Task Scheduler and it 
#    passed the task to Executor
# D. Executor gives the required output.

# Before Spark RDDs, large volumes of data were processed using in-disk computation, which is slower than
# in-memory computation

# The existing distributed computing systems (viz. MapReduce) need to store data in some intermediate stable distributed
# store, namely HDFS. It makes the overall computations of jobs slower as it involves multiple IO operations, replications and 
# serializations in the process

# Spark RDDs have various capabilities to handle huge volumes of data by processing them parallely over 
# multiple logical partitions. We can create an RDD, anytime, but Spark RDDs are executed only when they are needed
# (lazy evaluation)

# Features of Spark RDDs:

# A. In-memory Computation:

# The data inside an RDD can be stored in memory for as long as possible

# B. Immutable or Read-Only:

# An RDD can't be changed once created and can only be transformed using transformations

# C. Lazy Evaluation:

# The data inside an RDD is not available or transformed until an action is executed that triggers the execution

# D. Cacheable:

# Cache stores the intermediate RDD results in memory only, i.e. the default storage of RDD cache is in memory
# to make access fast.

# E. Parallel Data Processing:

# RDDs can process data in parallel

# F. Typed:

# RDD records have types e.g. Int, Long, etc gives better data readibility to check long type data etc.

# creating a spark context

conf= SparkConf().setAppName('SparkContext').setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)

# -- The "conf" object is the configuration for a Spark application
# -- We define the App Name and the Master URL in it
# -- "sc" is an object of SparkContext

values = [1,2,3,4,5]
rdd = sc.parallelize(values) # parallelize take collections like list etc.

# printing the RDD values:
print(rdd.take(5))

# Uploading a file to Google Colab:

# from google.colab import files
# uploaded = files.upload()

# # initializing an RDD using a text file:

# rdd = sc.textFile("PySpark1.txt")

# # printing the text from RDD

# print(rdd.collect())

###################################################################################

# RDD Persistence and Caching:

# -- Spark RDDs are lazily evaluated; thus when we wish to use the same RDD multiple times, it
# results in recomputing the RDD

# -- To avoid computing an RDD multiple times, we can ask Spark to persist the data

# -- In this case, the node that computes the RDD stores its partitions

# -- If a node has data persisiting on it fails, Spark will recompute the lost partitions of the data when required

# -- RDDs comes with a method called unpersist() that lets us manually remove them from the cache
conf= SparkConf().setAppName('SparkContext1').setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)
aba = sc.parallelize(range(1,10000,2))
print(aba.persist())
 
 # Caching:

# -- It is very useful when data is accessed repeatedly, such as, querying a small 'hot' dataset or running an iterative algorithm

# -- Cache is fault-tolerant

# -- Let is revisit our example and mark our linesWithSpark dataset to be cached
conf= SparkConf().setAppName('SparkContext2').setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)
abb = sc.parallelize(range(1,100,2))
print(abb.cache())

##################################################################################

# Operations on RDDs:

# There are two types of data operations we can perform on an RDD, transformations and actions

# -- A transformation will return a new RDD as RDDs are generally immutable

# -- An action will return a value

# Transformations are lazy operations on an RDD that create one or more new RDDs.

# RDD transformations return a pointer to the new RDD and allow us to create dependencies between RDDs.
# Each RDD in a dependency chain (a string of dependencies) has a function for calculating its data and 
# a pointer (dependency) to its parent RDD.

# Given below is a list of RDD Transformations:

# map
# flatMap
# filter
# mapPartitions
# mapPartitionsWithIndex
# sample
# union
# intersection
# distinct
# groupBy
# keyBy
# Zip
# zipwithIndex
# Coalesce
# Repartition
# sortBy

# Map:

# it passes each element through a function.

x=sc.parallelize(["spark","rdd","example","sample","example"]) 
y=x.map(lambda x: (x,1))
print(y.collect())

# flatMap:

# It is like map, but here each input item can be mapped to 0 or more output items (so, a function should
# return a sequence rather than a single item)


rdd = sc.parallelize([2,3,4])
print(sorted(rdd.flatMap(lambda x: range(1,x)).collect()))

# filter:

# Returns a collection of elements on the basis of the condition provided in the function

rdd = sc.parallelize([1,2,3,4,5])
print(rdd.filter(lambda x: x%2 ==0).collect())

# Sample:

# -- it samples a fraction of the data with or without replacement, using a given random number generator seed.

# -- We can add following parameters to sample method:

# A. withReplacement: Elements can be sampled multiple times (replaced when sampled out)
# B. fraction: Makes the size of the sample as a fraction of the RDD's size without replacement
# C. seed: A number as a seed for the random number generator

parallel=sc.parallelize(range(9))
print(parallel.sample(True,.2).count())
print(parallel.sample(False,1).collect())

# Union:

# It returns the union of two RDDs after concatenating their elements.

parallel = sc.parallelize(range(1,9))
par = sc.parallelize(range(5,15))
print(parallel.union(par).collect())

# Intersection:

# similar to union, but returns the intersection of two RDDs

parallel = sc.parallelize(range(1,9))
par = sc.parallelize(range(5,15))
print(parallel.intersection(par).collect())

# Distinct:

# Returns a new RDD with distinct elements within the source data

parallel = sc.parallelize(range(1,9))
par = sc.parallelize(range(5,15))
print(parallel.union(par).distinct().collect())

# sortBy:

# It returns the RDD sorted by the given key function.

y = sc.parallelize([5,7,1,3,2,1])
print(y.sortBy(lambda c: c, True).collect())

z=sc.parallelize([("H",10),("A",26),("Z",1),("L",5)])
print(z.sortBy(lambda c:c,False).collect())

# MapPartitions:

# can be used as an alternative to map() and foreach()

# mapPartition() is called once for each partition unlike map() and 
# foreach(), which are called for each element in the RDD

rdd = sc.parallelize([1,2,3,4],2)
def f(iterator): yield sum(iterator)
print(rdd.mapPartitions(f).collect())

rdd = sc.parallelize([1,2,3,4],4) 
def f(splitIndex,iterator): yield splitIndex
print(rdd.mapPartitionsWithIndex(f).sum())

# groupBy:

# Returns a new RDD by grouping objects in the existing RDD using the given grouping key

rdd = sc. parallelize([1,1,2,3,5,8])
result= rdd.groupBy(lambda x: x%2).collect()
print(sorted([(x,sorted(y)) for (x,y) in result]))

# keyBy:

# It returns a new RDD by changing the key of the RDD element using the given key object

x = sc.parallelize(range(0,3)).keyBy(lambda x: x*x)
y = sc.parallelize(zip(range(0,5),range(0,5)))
print([(x,list(map(list,y))) for x,y in sorted(x.cogroup(y).collect())])

# zip joins two RDDs by combining the i^{th} part of either partition with each other.
# Returns an RDD formed from this list and another iterable collection by combining the corresponding elements in pairs.
# If one of the two collections is longer than the other, its remaining elements are ignored.

x = sc.parallelize(range(0,5))
y = sc.parallelize(range(1000,1005))
print(x.zip(y).collect())
print(sc.parallelize(["a","b","c","d"],3).zipWithIndex().collect())

# repartition:

# Used to either increase or decrease the number of partitions in an RDD.

# Does a full shuffle and creates new partitions with the data that are distributed evenly.

rdd = sc.parallelize([1,2,3,4,5,6,7],4)
print(sorted(rdd.glom().collect()))
print(rdd.repartition(2).glom().collect())

# Coalesce:

# This method is used to reduce the number of partitions in an RDD.

print(sc.parallelize([1,2,3,4,5],3).glom().collect())
print(sc.parallelize([1,2,3,4,5],3).coalesce(2).glom().collect())

# Difference Between Coalesce() and Repartition():

# A. Coalesce() uses the existing partitions to minimize the amount of data that's shuffled
#    Repartition() creates new partitions and does a full shuffle.

# B. Coalesce results in partitions with different amounts of data (at times with much different sizes)
#    Repartition results in roughly equal-sized partitions

# C. Coalesce() is faster and Repartition() is not so faster 

# Given below is a list of commonly used RDD actions:

# Reduce (func)
# first
# takeOrdered
# take
# count
# collect
# collectMap
# saveAsTextFile
# foreachPartition
# Foreach
# Max
# Min
# Sum
# Mean
# Variance
# stdev
from operator import add
print(sc.parallelize([1,2,3,4,5]).reduce(add))
print(sc.parallelize((2 for _ in range(10))).map(lambda x: 1).cache().reduce(add))
print(sc.parallelize([2,3,4]).first())
nums = sc.parallelize([1,5,3,9,4,0,2])
print(nums.takeOrdered(5))
nums = sc.parallelize([1,5,3,9,4,0,2])
print(nums.take(5))
nums = sc.parallelize([1,5,3,9,4,0,2])
print(nums.count())

# Collect:

# Returns the elements of the dataset as an array back to the driver program

# Should be used wisely as all worker nodes return the data to the driver node

# If the dataset is huge in size then this may result in an OutOfMemoryError

c = sc.parallelize(["Gnu","Cat","Rat","Dog","Gnu","Rat"],2)
print(c.collect())
c = sc.parallelize(["Gnu","Cat","Rat","Dog","Gnu","Rat"],2)
print(c.distinct().collect())

# "SaveAsTextfile" - filepath:


# Writes the entire RDD's dataset as a text file on the path specified
# in the local filesystem or HDFS:

a=sc.parallelize(range(1,10000),3)
a.saveAsTextFile("/home/ankit/Desktop/ml/PySpark/data")
x=sc.parallelize([1,2,3],3)
x.saveAsTextFile("/home/ankit/Desktop/ml/PySpark/sample1.txt")

# foreach:

# Passes each element in an RDD through the specified function.

def f(x): print(x)
print(sc.parallelize([1,2,3,4,5]).foreach(f))

def f(iterator):
  for x in iterator:
     print(x)
print(sc.parallelize([1,2,3,4,5]).foreachPartition(f))

# max,min,sum,mean,variance and stdev:

numbers = sc.parallelize(range(1,100))
print(numbers.sum())
print(numbers.min())
print(numbers.variance())
print(numbers.max())
print(numbers.mean())
print(numbers.stdev())

# RDD Functions:

# cache() --> caches an RDD to use without computing again
# collect() --> returns an array of all elements in an RDD
# countByValue() --> returns a mao with the number of times each value occurs
# distinct() --> returns an RDD containing only distinct elements
# filter() --> returns an RDD containing only those elements that match wise the function f
# foreach() --> Applies the function f to each elements of an RDD
# persist() --> sets an RDD with the default storage level (MEMORY_ONLY); sets the storage level that caches the RDD TO BE STORED AFTER IT IS COMputed (different storage level is there in StorageLevel)
# sample() --> returns an RDD of that fraction
# toDebugString() --> Returns a handy function that outputs the recursive steps of an RDD
# count() --> Returns the number of elements in an RDD
# unpersist()  --> Removes all the persistent blocks of an RDD from the memory/disk
# union() --> returns an RDD containing elements of 2 RDDs; duplicates are not removed

a=sc.parallelize([1,2,3,4,5,6,7,8,2,3,3,3,1,1,1])
print(a.countByValue())

a=sc.parallelize(range(1,19),3)
b=sc.parallelize(range(1,13),3)
c=a.subtract(b)
print(c.toDebugString())

# Creating Paired RDDs (working with key-value pair):

# -- There are a number of ways to get paired RDDs in spark.

# -- There are many formats that directly return paired RDDs for their 
# key-value data; in other cases, we have regular RDDs that need to be turned into paired RDDs

# -- We can do this by running a map() function that returns key-value pairs


rdd = sc.parallelize([("a1","b1","c1","d1","e1"),("a2","b2","c2","d2","e2")])
result = rdd.map(lambda x: (x[0],list(x[1:])))
print(result.collect())

# RDD Lineage is a graph of all parent RDDs of an RDD.
# It is built by applying transformations to the RDD and creating a logical execution plan.

# rdd.toDebugString # to print rdd image


# An RDD lineage graph is a graph of transformations that need to be executed after an action has been called.
# We can create an RDD lineage graph using the RDD.toDebugString method.

# WordCount using RDD concepts:

# -- it returns the frequency of every word.

rdd = sc.textFile("PySpark.txt")
nonempty_lines = rdd.filter(lambda x: len(x)>0)
words = nonempty_lines.flatMap(lambda x: x.split(' '))
wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)

for word in wordcount.collect():
	print(word)

# RDD Partioning:

# -- A partition is a logical chunk of a large distributed dataset

# -- Spark manages data using partitions that help parallelize distributed data processing with minimal network traffic 
#    for sending data between executors.

# -- By default, Spark tries to read data into an RDD from the nodes that are close to it.

# -- Since Spark usually accesses distributed partitioned data, to optimize transformations, it creates partitions 
#    that can hold the data chunks

# -- RDDs get partitioned automatically without a programmer's intervention

# -- However there are times when we would like to adjust the size and number of partitions or the partitioning scheme
  
# -- We can use the def getPartition: Array[Partition] method on an RDD to know the number of partition in the RDD

# (102) RDD Partitioning Types:


# A. Hash Partitioning
# B. Range Partitioning

# Hash Partitioning is a partitioning is a partitioning technique where a hash key is used to distribute elements evenly acrosss different partitions.

# Some Spark RDDs have keys that follow a particular order; for such RDDs, range partitioning is an efficient partitioning technique/
# In the range partitioning method, tuples having keys within the same range will appear on the same machine
# Keys in a RangePrtitioner are partitioned based on the set of sorted range of keys and ordering of keys.

# -- Custominzing partitioning is possible only on paired RDDs.    