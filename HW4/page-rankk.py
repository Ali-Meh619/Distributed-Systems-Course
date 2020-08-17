import os
import sys
import re
import itertools
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from operator import add
import time

import shutil ##we mustn't have result file at first

try:
    shutil.rmtree('./results')
    print('Deleted the \'results\' directory')
except:
    pass



os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'




conf = SparkConf()
spark = SparkSession.builder.master('local[4]').appName('ranking').getOrCreate()
sc = spark.sparkContext
sc.setCheckpointDir("./checkpoints")

lines = sc.textFile(sys.argv[1],4) # Inputs "dataset.txt"
ap = float(sys.argv[2]) # Inputs are always in string form
num = int(sys.argv[3])
beta = float(sys.argv[4])




def parse(x):#for parsing source node and its datas
    r=x.split()
    id=r[0]
    data=r[1:]
    return id,data


def node(x):
    r=x.split()
    f=r[1]
    return f

def nod(x):
    r=x.split()
    f=r[0]
    return f


def filterr(l):#filter and just keeping outbound wieghts and normalizing weights
    
    x1=l[0]
    x2=l[1][::2]
    c=len(x2)
    t=0
    
    for i in range(c):
        t=t+float(x2[i][1])
        
    for i in range(c):
        
        x2[i][1]=float(x2[i][1])/t
        
    

    return x1,x2

n1=lines.map(lambda l:node(l))
n2=lines.map(lambda l:nod(l))
n3=(n1+n2).distinct()#all the nodes that we have to handle

a=n3.count()
##


link=lines.map(lambda l:parse(l))
##
rank=n3.map(lambda y:[y,1/a])#initial ranks
use=link.join(rank).reduceByKey(lambda x,y:x+y).map(lambda r:filterr(r)).cache()
#nodes with outband weights,here we use cache because we need this rdd later


def compute(x):
    id=x[0]
    x1=x[1][0][0]
    x2=x[1][0][1]
    x3=x[1][1]
    c=len(x1)
    
    for i in range(c):
        
        if x1[i][0]!=id:
                                
            yield(x1[i][0],x3*x1[i][1]-x2)
            
        else:
            
             yield(x1[i][0],x3*x1[i][1])



for j in range(num):
    
    o=rank.map(lambda x:(x[0],x[1]*(1/(a-1)))).map(lambda x:x[1]).sum()

    h=rank.map(lambda x:(x[0],o-x[1]*(1/(a-1))))

    p=rank.map(lambda x:(x[0],(x[1]*(1/(a-1)))))

    y=use.join(p).join(rank).flatMap(lambda x:compute(x)).reduceByKey(add)
    
    pi=y.join(h).map(lambda x:(x[0],(1-ap)*(x[1][0]+x[1][1])+(o*ap*(1-(1/a)))))#pagerank formulla
    pi.checkpoint()
    if pi.join(rank).map(lambda x:abs(x[1][0]-x[1][1])).sum() <= beta:#for computing error
        
        rank=pi
        break;
        
    rank=pi

pi = rank.coalesce(1)
pi.saveAsTextFile('./results')

sc.stop()














    
        
    
    
    
    


    
    

    


