# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 02:57:35 2020

@author: Ali Mehrabian
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 01:19:54 2020

@author: Ali Mehrabian
"""

import numpy as np
import os

v=np.array([[0,1,20],[0,2,100],[1,2,30],[1,3,10],[1,4,20],[3,2,40]])

a=np.size(v,axis=0)
b=max(v[:,0]);
k=np.zeros((b+2,b+2))

for i in range(a):
    
    k[v[i,0],v[i,1]]=v[i,2];
    k[v[i,1],v[i,0]]=v[i,2];
    

t=np.array(np.where(k>0))
d=np.size(t,axis=1)

qq=np.zeros((b+2,b+2),dtype=np.uint16)

for i in range(d):
    
    qq[t[0,i],t[1,i]]=int(9000+100*t[0,i]+10+t[1,i]);
    

import json
class message:
    
    def __init__(self,type,value):
        
        self.value = value
        self.type=type

import random as r

h=[];
for i in range(100):
    
    h.append(r.expovariate(1))

n=np.random.randint(0,2)





import socket
import threading
import time
import pickle
 
ENCODING = 'utf-8'
  
class ab(threading.Thread):
 
    def __init__(self,table,i,q):
        threading.Thread.__init__(self)
        
        
        self.table = table
        self.d=np.size(table,axis=0)
        self.p=0;
        self.id=i;
        self.i=1;
        self.mark=0;
        self.port=q;
        self.pb=-1;
        n=self.table[self.id,:]
        u1=np.where(n>0)
        self.u2=np.size(u1)
        self.op=np.zeros((1,self.u2))
        o1=np.array(u1)
        o2=o1[0];
        self.r=o2;
        bb=self.port[self.id,u1]
        dd=self.port[u1,self.id]
        d1=dd[0]
        b1=bb[0]
        k1=np.array(u1)[0]
        h2=np.vstack((b1,k1))
        
        
        m=self.table[self.id,u1]
        m1=m[0]
        b=np.array(range(self.u2))
        h1=np.vstack((m1,b))
        h3=np.vstack((h1,h2))
        self.h=np.vstack((h3,d1))
        
        
        oo=[];
        
        for i in range(100):
            
    
            oo.append(r.expovariate(1))
        
        
        self.f=oo;
        
        am=[]
        for i in range(300):
            
    
            am.append(np.random.randint(0,self.u2))
        
        self.gg=am

        
        self.h=self.h.astype(int)
        fm=np.random.randint(0,self.u2)
        self.pm=fm
        
    
    def listen(self,port,w):

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1',int(port)))
        sock.listen(10)

        while True:

            connection, client_address = sock.accept()

            while True:
                
                
                data1 = connection.recv(1024);
                
                
                if data1 :
                        
                    kc=pickle.loads(data1)
  
                    self.f=np.minimum(self.f,kc)

                    
                    
                if not data1:

                    break;
            if self.p==port:
                
                
                        
                break
                
        return                
                    

    def send(self):
        
        
        
    
        while True:

            if self.i<=300:
                
                n=self.gg[self.i-1]
                p=self.h[4,n];
            
            if self.i<300:
                
                a=(self.h[0,n])/1000;
                time.sleep(a)
                arr=(self.f)
                da = pickle.dumps(arr)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('127.0.0.1',p))
                s.sendall(da) 
                self.i=self.i+1
                s.close()

            if self.i==300:
    
                a=(self.h[0,n])/1000;
                time.sleep(a)
                arr=(self.f)
                da = pickle.dumps(arr)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('127.0.0.1', p))
                s.sendall(da)    
                s.close()
                self.mark=1;
               
            if self.i==300:
                a=np.mean(self.f);       
                b=np.ceil(1/a)
                print('Node '+str(self.id)+' Estimated Network size '+str(b)+'\n');
                self.pb=1
                self.p=p
                break;    
        
        return        

    def run1(self):
        
        t=[]
        y=self.u2
        
        for i in range(y):
            t1=threading.Thread(target=self.listen, args=(self.h[2,i], i,))
            
                    
            t1.start()
            t.append(t1)

        t2=threading.Thread(target=self.send)
        t2.start()
        t.append(t2)
        
        for n in t:
            
            n.join()
         



def dia(k):
    threads=[];
    for j in range(np.size(k,axis=0)):
        
        
        
        s=ab(k,j,qq);
        t=threading.Thread(target=s.run1)
        t.start()
        threads.append(t)
    for thread in threads:
        thread.join()
        
dia(k)                   