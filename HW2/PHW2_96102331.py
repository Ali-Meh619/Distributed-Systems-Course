# -*- coding: utf-8 -*-
"""
Created on Fri May  1 19:01:24 2020

@author: Ali Mehrabian
"""

import numpy as np
import os

v=np.array([[0,1,2],[0,2,1],[1,2,3],[1,3,1],[1,4,2],[3,2,4],[4,3,2]])

a=np.size(v,axis=0)
b=max(v[:,0]);
k=np.zeros((b+1,b+1))

for i in range(a):
    
    k[v[i,0],v[i,1]]=v[i,2];
    k[v[i,1],v[i,0]]=v[i,2];
    


t=np.array(np.where(k>0))
d=np.size(t,axis=1)

qq=np.zeros((b+1,b+1),dtype=np.uint16)

for i in range(d):
    
    qq[t[0,i],t[1,i]]=int(9000+100*t[0,i]+10+t[1,i]);
    
    
        


import json

class message:
    
    def __init__(self,type,value):
        
        self.value = value
        self.type=type
        
        
 
    
import socket
import threading
import time
 
ENCODING = 'utf-8'
  
class ab(threading.Thread):
 
    def __init__(self,table,i,sink,q):
        threading.Thread.__init__(self)
        
        
        self.table = table
        self.d=np.size(table,axis=0)
        self.sink=sink;
        self.id=i;
        self.parent=-1;
        self.mark=0;
        self.port=q;
        self.ok=0;
        
        
        
        
        
        n=self.table[self.id,:]
        u1=np.where(n>0)
        self.u2=np.size(u1)
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
        
        
        self.f=[0];
        self.j1=np.zeros((self.u2,6))
        
        
            
            
            
            
        self.j1=self.j1.astype('str')
        
        if self.id==self.sink:
            
            self.j1[:,2]=1;
        
        self.h=self.h.astype(int)
    
    def listen(self,port,w):
        
        
        jj=0
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1',int(port)))
        sock.listen(10)
        
            
            
        while True:
            
            
            
            connection, client_address = sock.accept()
                
            while True:
                
                
                data1 = connection.recv(1024);
                
                
                if data1 :
                        
                    mm=message(**json.loads(data1,encoding='utf-8'));
                    self.j1[w,3]=mm.value
                    self.j1[w,4]=mm.type
                        
                        
                    if self.mark!=1 and self.j1[w,4]=='search' and self.sink!=self.id and self.parent==(-1): 
                        
                        self.parent=self.j1[w,3];
                        self.j1[:,2]=1;
                        
                            
                            
                            
                    if self.j1[w,4]=='non-parent':
                            
                        self.j1[w,1]=1;
                        
                    if self.j1[w,4]=='con-cast':
                            
                        self.j1[w,0]=0;
                        self.j1[w,1]=1;
                            
                        self.j1[w,5]=1;
                            
                        self.f.append(int(self.j1[w,3])+1);
                        
                
                    
                if not data1:
                    
                    ko=self.j1[:,1]
                    ko=ko.astype('float')
                    if self.id==self.sink and np.sum(ko)==self.u2:
                            print(str(self.id)+ ' recieved con-cast message from '+str(self.h[3,w])+'\n')
                            print('netwok size is '+str(sum(self.f)+1))
                            self.ok=1;
                            break;
                                
                        
                        
                    if self.j1[w,4]=='parent':
                            
                        self.j1[w,0]=1;
                
                    
                    if self.j1[w,4]=='parent':
                        
                            
                        print(str(self.id)+ ' recieved parent message from '+str(self.h[3,w])+'\n')
                        break;
                    if self.j1[w,4]=='non-parent':
                            
                        print(str(self.id)+ ' recieved non-parent message from '+str(self.h[3,w])+'\n')
                        break;    
                            
                    if (self.j1[w,4]=='search' and self.parent!=(-1) and self.sink!=self.id) or (self.j1[w,4]=='search' and self.sink==self.id):
                            
                        print(str(self.id)+ ' recieved search message from '+str(self.h[3,w])+'\n')
                        break;
                            
                    if self.j1[w,4]=='con-cast':
                    
                        print(str(self.id)+ ' recieved con-cast message from '+str(self.h[3,w])+'\n')
                        break;
                    
                    
                
                    
                    
                        
                        
        return                
                    

    def send(self,port,x):
        
        port=int(port)
    
        while True:
            if (self.j1[x,4]=='search' and self.mark==1) or (self.j1[x,4]=='search' and self.id==self.sink):
                time.sleep(int(self.h[0,x]))
                m = message('non-parent',self.id)
                mm=json.dumps(m.__dict__).encode("utf-8")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('127.0.0.1', port))
                s.sendall(mm)
                self.j1[x][3]=0
                self.j1[x][4]=0
                self.mark=1;    
                s.close()
            
               
            if self.j1[x,4]=='search' and self.id!=self.sink and self.mark!=1:
                time.sleep(int(self.h[0,x]))
                m = message('parent',self.id)
                mm=json.dumps(m.__dict__).encode("utf-8")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('127.0.0.1', port))
                s.sendall(mm)
                self.j1[x,3]=0
                self.j1[x,4]=0
                self.mark=1;
                s.close()
                
                
                    
            if self.j1[x,2]=='1':
                time.sleep(int(self.h[0,x]))
                m = message('search',self.id)
                mm=json.dumps(m.__dict__).encode("utf-8")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('127.0.0.1', port))
                s.sendall(mm)
                self.j1[x,2]=0;
                s.close()
                
             
            
            ko=self.j1[:,1]
            ko=ko.astype('float')
            if np.sum(ko)==self.u2 and self.mark==1 and int(self.parent)==self.h[3,x]:    
                time.sleep(int(self.h[0,x]))
                o1=sum(self.f)
                m = message('con-cast',o1)
                mm=json.dumps(m.__dict__).encode("utf-8")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('127.0.0.1', port))
                s.sendall(mm)
                self.j1[x,2]=0;
                s.close()
                break;
            
        return            
    def run1(self):
        
        t=[]
        y=self.u2
        
        for i in range(y):
            t1=threading.Thread(target=self.listen, args=(self.h[2,i], i,))
            t2=threading.Thread(target=self.send, args=(self.h[4,i], i,))
            t2.start()        
            t1.start()
            t.append(t1)
            t.append(t2)
        
        for n in t:
            
            n.join()
        
        
        if self.ok==1:
            
            os._exit(0);
        
        
def dia(k):
    threads=[];
    for j in range(np.size(k,axis=0)):
        
        
        
        s=ab(k,j,0,qq);
        t=threading.Thread(target=s.run1)
        t.start()
        threads.append(t)
    for thread in threads:
        thread.join()
        
dia(k)            

    