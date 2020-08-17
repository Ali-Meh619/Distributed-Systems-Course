# -*- coding: utf-8 -*-
"""
Created on Fri May 22 08:58:22 2020

@author: Ali Mehrabian
"""

# -*- coding: utf-8 -*-
"""
Created on Fri May 22 08:56:14 2020

@author: Ali Mehrabian
"""

import numpy as np
import os

v=np.array([[0,1,1],[0,2,1.2],[1,0,1.5],[1,2,1],[2,0,1],[2,1,2.5]])

a=np.size(v,axis=0)
b=int(max(v[:,0]));


y=b+1

k=np.zeros((y,y))

p=0;

for i in range(2*y):

    k[p,int(v[i,1])]=v[i,2];
    
    if i%2!=0:
        
        p=p+1
    
        


t=np.array(np.where(k>0))
d=np.size(t,axis=1)

qq=np.zeros((y,y),dtype=np.uint16)

for i in range(d):
    
    qq[t[0,i],t[1,i]]=int(9000+100*t[0,i]+10+t[1,i]);
        
w=qq[:,0]

t0=np.array([10,6,5])
t1=np.array([14,5,5])
t2=np.array([18,6,6])
y1=np.vstack((t0,t1))
y2=np.vstack((y1,t2))

import socket
import threading
import time
import pickle
n=3
 
ENCODING = 'utf-8'
  
class ab(threading.Thread):
 
    def __init__(self,table,i,to,port,n):
        threading.Thread.__init__(self)
        
        
        self.table = table
        self.d=np.size(table,axis=0)
        self.p=0;
        self.id=i;
        self.to=to[self.id,:];
        self.lead_buff=[];
        self.leader=0
        self.leader_id=0;
        self.ney=0;
        self.size=n;
        self.my_leader=0;
        self.my_leader_v=0;
        self.decide=0
        self.ss=0
        self.state=0
        
        self.max_round=0;
        self.decided=0;
        self.final=0
        
        self.i=1;
        self.mark=0;
        self.port=port;
        self.e=0;
        self.end=0
        
  
        n=self.table[self.id,:]
        u1=np.where(n>0)
        self.u2=np.size(u1)
        self.op=np.zeros((1,self.u2))
        o1=np.array(u1)
        
        jj=o1[0]+1
        delay=n[u1]
        send=self.port[self.id,u1][0];
        lis=self.port[u1,self.id][0]
        self.h=[jj,delay,send,lis]
  
        o2=o1[0];
        self.r=o2;
        bb=self.port[self.id,u1]
        dd=self.port[u1,self.id]
        
        self.d1=dd[0]
        self.b1=bb[0]
        
 
    def listen(self,i):
        
        
        p0=self.h[3]
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1',int(p0[i])))
        sock.listen(10)
        
        
            
            
        while True:
            
            
            
            connection, client_address = sock.accept()
                
            while True:
                
                
                data1 = connection.recv(1024);
                
                
                if data1 :
                        
                    kc=pickle.loads(data1)
                    
                    
                    if kc['type']=="POTENTIAL_LEADER" and self.max_round<kc['value']:
                        
                        self.leader_m=kc['value']
                        self.state=kc['type']
                        ko=list(self.h[0])
                        
                        self.ss=ko.index(kc['nid'])
                        
                        
                        
                        
                    if kc['type']=="POTENTIAL_LEADER_ACK":
                        
                        
                        self.lead_buff.append(kc['value'][0])
                        self.lead_buff.append(kc['value'][1])
                        
                        self.ney=self.ney+1;
                        
                    if kc['type']=="V_PROPOSE":
                        
                        self.state=kc['type']
                        self.my_leader=kc['value'][0]
                        self.my_leader_v=kc['value'][1]
                        
                    if kc['type']=="V_PROPOSE_ACK":
                        
                        self.ney=self.ney+1;
                    
                    if kc['type']=="V_DECIDE":
                        self.decided=1;
                    
   
                if not data1:
                    if kc['type']=="POTENTIAL_LEADER":
                        
                        print('node '+str(self.id+1)+': '+str(kc['nid'])+' POTENTIAL_LEADER '+str(kc['value'])+'\n');
                        break;
        
                    if kc['type']=="POTENTIAL_LEADER_ACK":
                        
                        

                        print('node '+str(self.id+1)+': '+str(kc['nid'])+' POTENTIAL_LEADER_ACK '+str(kc['value'][0])+', '+str(kc['value'][1])+'\n');
                        break;

                    if kc['type']=="V_PROPOSE":
                        
                        print('node '+str(self.id+1)+': '+str(kc['nid'])+' V_PROPOSE '+str(kc['value'][0])+','+str(kc['value'][1])+'\n');
                        break;
                    
                    if kc['type']=="V_PROPOSE_ACK":
                        
                        print('node '+str(self.id+1)+': '+str(kc['nid'])+' V_PROPOSE_ACK '+str(kc['value'])+'\n');
                        break;
                        
                    if kc['type']=="V_DECIDE":
                        self.end=1;
                        print('node '+str(self.id+1)+': '+str(kc['nid'])+' V_DECIDE '+str(kc['value'])+'\n');

                        break;
            
            
            #if self.end==1:
             #   break;
            
        return                
                    

    def send(self):
        
        
        
    
        while True:
            
            if self.end==1:
                break;
            
                
            p0=self.h[2]
            p=p0[self.ss]
            if self.state=="POTENTIAL_LEADER" and self.end!=1:
                
                if self.max_round==0 and self.state=="POTENTIAL_LEADER" and self.end!=1:
                    
                    f=self.h[1]
                    time.sleep(f[self.ss])
                    self.max_round=self.leader_m
                    self.state=0
                    data={};
                    
                    data['nid']=self.id+1
                    data['type']="POTENTIAL_LEADER_ACK"
                    data['value']=0,-1
                    x=pickle.dumps(data)
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect(('127.0.0.1', int(p)))
                    s.sendall(x)    
                    s.close()
        
        
                
                if self.max_round!=0 and self.state=="POTENTIAL_LEADER" and self.end!=1:
                    f=self.h[1]
                    time.sleep(f[self.ss])
                    data={};
                    
                    self.state=0
                    data['nid']=self.id+1
                    data['type']="POTENTIAL_LEADER_ACK"
                    data['value']=self.my_leader,self.my_leader_v
                    self.max_round=self.leader_m
                    x=pickle.dumps(data)
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect(('127.0.0.1', int(p)))
                    s.sendall(x)    
                    s.close()
        
            if self.state=="V_PROPOSE" and self.end!=1:
                f=self.h[1]
                time.sleep(f[self.ss])
                data={};
                self.state=0
                data['nid']=self.id+1
                data['type']="V_PROPOSE_ACK"
                data['value']=-1
                x=pickle.dumps(data)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('127.0.0.1', int(p)))
                s.sendall(x)    
                s.close()
                
                
    
        return
    
    def sendL(self,ii):
        while True:
           
            if self.decide==1 or self.end==1:
                break;

            time.sleep(self.to[0])
            if self.decide==1 or self.end==1:
                break;
            data={};
            data['nid']=self.id+1
            data['type']="POTENTIAL_LEADER"
            data['value']=self.max_round+1
            x=pickle.dumps(data)
            u0=self.h[2]
            f=self.h[1]    
            time.sleep(f[ii])
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('127.0.0.1',int(u0[ii])))
            s.sendall(x)    
            s.close()
            xx=time.time()
            while True:                
                if time.time()-self.to[1]>xx:
                    break;        
                if self.ney>=((self.size//2)+1):            
                    self.leader=1;
                        
                    if max(self.lead_buff)==0:
                        data={};
                        data['nid']=self.id+1
                        data['type']="V_PROPOSE"
                        data['value']=self.max_round+1,self.size*(self.id+1)
                        self.final=self.size*(self.id+1)
                        x=pickle.dumps(data)
                        u0=self.h[2]
                        f=self.h[1]       
                        time.sleep(f[ii])
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect(('127.0.0.1', int(u0[ii])))
                        s.sendall(x)    
                        s.close()
                        self.ney=0;
                        break;
                
                
                
                    if max(self.lead_buff)!=0:
                        data={};
                        data['nid']=self.id+1
                        data['type']="V_PROPOSE"
                        data['value']=self.max_round+1,max(self.lead_buff)
                        self.final=max(self.lead_buff)
                        x=pickle.dumps(data)
                        u0=self.h[2]
                        f=self.h[1]
                        time.sleep(f[ii])
                        s= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect(('127.0.0.1',int(u0[ii])))
                        s.sendall(x)    
                        s.close()
                        self.ney=0;
                        break;
                
                    
                
        
            fm=time.time();
            
            while True:
                
                if time.time()-self.to[2]>fm:
                    break;

                if self.ney>=((self.size//2)+1):
                    data={};
                    data['nid']=self.id+1
                    data['type']="V_DECIDE"
                    data['value']=self.final
                    self.decide=1;
                    self.end=1;
                    x=pickle.dumps(data)
                    u0=self.h[2]
                    f=self.h[1]
                    time.sleep(f[ii])
                    soo = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    soo.connect(('127.0.0.1',int(u0[ii])))
                    soo.sendall(x)    
                    soo.close()
                    break;
                    
            if self.decide==1:
                
                
                ff=self.h[1]
                g0=max(list(ff))
                time.sleep(g0+1)
                os._exit(0);        
            
            
            

    def run1(self):
        
        t=[]
        y=self.u2
        
        for i in range(y):
            t1=threading.Thread(target=self.listen, args=(i,))
            
                    
            t1.start()
            t.append(t1)

        t2=threading.Thread(target=self.send)
        t2.start()
        t.append(t2)
        for i in range(y):
            t3=threading.Thread(target=self.sendL, args=(i,))
            t3.start()
            t.append(t3)
            
        
        
        for n in t:
            
            n.join()
        
            
            
        



def dia(k):
    threads=[];
    for j in range(np.size(k,axis=0)):
        
        
        
        s=ab(k,j,y2,qq,n);
        t=threading.Thread(target=s.run1)
        t.start()
        threads.append(t)
    for thread in threads:
        thread.join()
        
dia(k)      