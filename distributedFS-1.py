#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging
import os.path
from collections import defaultdict
from errno import ENOENT,ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
import math

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


import logging, xmlrpclib, pickle

from xmlrpclib import Binary

from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import hashlib

if not hasattr(__builtins__, 'bytes'):
    bytes = str

######################################  OBTAIN SERVER AND CHECK IF ALIVE #################################################################
#determine server for particular path
def getserver(path):
    path=path.encode()
    m = hashlib.sha256()
    m.update(path)
    str = m.hexdigest()
    dec = int(str,16)
    print ( " In decimal : ")
    print (dec%snum)
    return (dec%snum)

#map servernumber to server
def mapserver(number):
    if(number==0):
	dataserver0 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport0)))
        return dataserver0
    elif(number==1):
        dataserver1 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport1)))
	return dataserver1
    elif(number==2):
        dataserver2 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport2)))
	return dataserver2
    elif(number==3):
	dataserver3 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport3)))
	return dataserver3
    elif(number==4):
        dataserver4 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport4)))
	return dataserver4
#determine if all servers alive
def arealive():
    flag_alive=0
    
    for server in servlist[0:snum]:
      if(isalive(server)):
          flag_alive=flag_alive+1
    if(flag_alive==snum):
      return True
    else:
      return False
    
#determine if server is alive
def isalive(server):
    try:
      server.checkalive()
    except:
      return False
    return True

#**************************************DATA FUNCTIONS*******************************************************************

#empty the datserver at path
def empty(server,key):
    print("Key is")
    print(key)
    return server.emptyserver(Binary(key))
#putdata in dataserver
def putdata(server,key,value):
    return server.put(Binary(key),Binary(pickle.dumps(value)))
#getdata from dataserver
def getdata(server,key):
    return pickle.loads(server.get(Binary(key)).data)
#get entire dataserver dictionary
def getdatafull(server):
    return pickle.loads(server.getwholedata().data)
#put entire dataserver dictionary
def putdatafull(server,value):
    return server.putwholedata(Binary(pickle.dumps(value)))

#****************************************REPLICA FUNCTIONS******************************************************************************
#empty the replica datastructure at path
def emptyreplica(server,key):
    print("Key is")
    print(key)
    return server.emptyreplica(Binary(key))
#putdata in replica dataserver
def putdata_replica(server,key,value):
    return server.put_to_replica(Binary(key),Binary(pickle.dumps(value)))
#getdata from dataserver
def getdata_replica(server,key):
    return pickle.loads(server.get_from_replica(Binary(key)).data)
#get entire dataserver replica dictionary
def getdatafull_replica(server):
    return pickle.loads(server.getwholedata_replica().data)
#put entire dataserver replica dictionary
def putdatafull_replica(server,value):
    return server.putwholedata_replica(Binary(pickle.dumps(value)))

#*******************************************CHECKSUM FUNCTIONS****************************************************************************
#get checksum
def getchecksum(server,key):
    return pickle.loads(server.get_checksum(Binary(key)).data)
#put checksum
def putchecksum(server,key,value):
    return server.put_checksum(Binary(key),Binary(pickle.dumps(value)))
#get checksum replica
def getchecksum_replica(server,key):
    return pickle.loads(server.get_checksum_replica(Binary(key)).data)
#put checksum replica
def putchecksum_replica(server,key,value):
    return server.put_checksum_replica(Binary(key),Binary(pickle.dumps(value)))
#empty checksum
def empty_checksum(server,key):
    return server.emptychecksum(Binary(key))
#empty checksum replica
def empty_checksum_replica(server,key):
    return server.emptychecksumreplica(Binary(key))

def calc_checksum(key):
    key=key.encode()
    m = hashlib.sha256()
    m.update(key)
    str = m.hexdigest()
    return str

#**************************************************INDEX OPERATIONS***********************************************************************
def resetindex(server):
    return server.reset()
def reset_rindex(server):
    return server.reset_replica()
def reset_checksum_index(server):
    return server.reset_checksum_index()
def reset_replica_checksum_index(server):
    return server.reset_checksum_rindex()
def increment_repindex(server):
    return server.inc_replicaindex()
def increment_repcheckindex(server):
    return server.inc_replicacheckindex()

#***********************************************PARENT CHILD OPERATIONS ********************************************************************
#put parent child information in metaserver
def putdata_direct(server,key,value):
    return server.putdirect(Binary(key),Binary(pickle.dumps(value)))
#get parent child information from metaserver
def getdata_direct(server,key):
    return pickle.loads(server.getdirect(Binary(key)).data)
def getdirectory(server):
    return pickle.loads(server.getwholedirect().data)
def putdirectory(server,value):
    return server.putwholedirect(Binary(pickle.dumps(value)))

#*****************************************************METADATA OPERATIONS******************************************************************************
#put file information in metaserver
def putdata_file(server,key,value):
    return server.put(Binary(key),Binary(pickle.dumps(value)))
#get file information from metaserver
def getdata_file(server,key):
    return pickle.loads(server.get(Binary(key)).data)
def getfile(server):
    return pickle.loads(server.getwholefile().data)
def putfile(server,value):
    return server.putwholefile(Binary(pickle.dumps(value)))
#*****************************************************************************************************************************************

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self):
        self.files = {}

#Since the value ofthe keys in dictionary data is now a list
        self.data = defaultdict(list)

#add a dictionary directory which contains keys as pathname of file or directory and value as parent
        self.directory=defaultdict(str)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        
        putdata_file(metaserver,'/',self.files['/'])
        putdata_direct(metaserver,'/',None)
        servernum=getserver('/')
        mappedserver=mapserver(servernum)
        
        
    def chmod(self, path, mode):
        files = getdata_file(metaserver,key)
        files['st_mode'] &= 0o770000
        files['st_mode'] |= mode
        files['st_ctime']=time()
	putdata_file(metaserver,path,files)
        return 0

    def chown(self, path, uid, gid):
        files = getdata_file(metaserver,key)
        files['st_uid'] = uid
        files['st_gid'] = gid
        files['st_ctime']=time()
        putdata_file(metaserver,path,files)

    def create(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        parent = os.path.dirname(path)
        self.directory[path]=parent
        self.fd += 1
        putdata_file(metaserver,path,self.files[path])
        putdata_direct(metaserver,path,self.directory[path])
	#putdata(dataserver,path,'')
        return self.fd

    def getattr(self, path, fh=None):
        files = getdata_file(metaserver,path)
        if files=={}:
            raise FuseOSError(ENOENT)
        return files
        

    def getxattr(self, path, name, position=0):
	files = getdata_file(metaserver,path)
        attrs = files.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = getdata_file(metaserver,path).get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):

        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        parent =os.path.dirname(path)
        self.directory[path]=parent
        self.files[parent]['st_nlink'] += 1
        self.data[path]=None
        putdata_file(metaserver,path,self.files[path])
        putdata_direct(metaserver,path,self.directory[path])
	#putdata(dataserver,path,self.data[path])
        

    def open(self, path, flags):

      #get metadata, update it and update on server
        originalmeta=getdata_file(metaserver,path)
        originalmeta['st_atime']=time()
        putdata_file(metaserver,path,originalmeta)
        self.fd += 1
        return self.fd

#read data requested by user as string by converting the list to string
    def read(self, path, size, offset, fh):
        for server in servlist[:snum]:
            if(isalive(server)):
              resetindex(server)
              reset_rindex(server)
              reset_checksum_index(server)
              reset_replica_checksum_index(server)
 
  #get dictionary containing metadata for the file to be read
        files=getdata_file(metaserver,path)
    
    #get the size of the data in the file
        size=files['st_size']
        print("Size of file being read")
        print(size)
        
    #get number of blocks of file data
        if((size%8) ==0):
          blocks = size/8
        else:
          blocks = math.floor((size/8))+1
        print("Number of blocks in file")
        print(blocks)

    #get starting server for the path
        servernum = getserver(path)
        replicanum1= (servernum+1)%snum
        replicanum2= (replicanum1+1)%snum
        text=[]
    #set a flag to say that checksum matched
        
        flag_alive=True
        flag_rep1_alive=True
        flag_rep2_alive=True

        while(blocks>0):
        
         mappedserver=mapserver(servernum)
         mappedreplica1=mapserver(replicanum1)
         mappedreplica2=mapserver(replicanum2)
         print(mappedserver)
         flag_alive=isalive(mappedserver) 
         flag_rep1_alive=isalive(mappedreplica1)
         flag_rep2_alive=isalive(mappedreplica2)

         if(flag_alive):
           print("Mapped server is alive")
           print(mappedserver)
           newblock=getdata(mappedserver,path)
           if calc_checksum(newblock)==getchecksum(mappedserver,path):
              print("Authentic data found in mapped server")
              text.append(newblock)
              if(isalive(mappedreplica1)):
                 increment_repindex(mappedreplica1)
                 increment_repcheckindex(mappedreplica1)
              if(isalive(mappedreplica2)):
                 increment_repindex(mappedreplica2)
                 increment_repcheckindex(mappedreplica2) 
              print("In mappedserver text is")
              print(text) 
           else:
              print("Data in mapped server found corrupted,retrieving data from replica")
              if(flag_rep1_alive):
                    print("In mapped replica 1")
                    print(mappedreplica1)
                    newblock=getdata_replica(mappedreplica1,path)
                    if calc_checksum(newblock)==getchecksum_replica(mappedreplica1,path):
                       print("Authentic data found in replica 1")
                       text.append(newblock)
                       if(isalive(mappedreplica2)):
                         increment_repindex(mappedreplica2)
                         increment_repcheckindex(mappedreplica2)
                    else:
                       if(flag_rep2_alive):
                         print("Data in replica 1 corrupted, checking in replica 2")
                         print("In mapped replica 2")
                         print(mappedreplica2)
                         newblock=getdata_replica(mappedreplica2,path)
                         if calc_checksum(newblock)==getchecksum_replica(mappedreplica2,path):
                             text.append(newblock)
                             print("In replica 2 after corruption, Text is")
                             print(text)        
         else:
            if(flag_rep1_alive):
              print("In mapped replica 1 because mappedserver dead")
              print(mappedreplica1)
              newblock=getdata_replica(mappedreplica1,path)
              if calc_checksum(newblock)==getchecksum_replica(mappedreplica1,path):
                 print("Authentic data found in replica 1,mappedserver dead")
                 print("Text is now")
                 print(text)
                 text.append(newblock)
                 if(isalive(mappedreplica2)):
                   increment_repindex(mappedreplica2)
                   increment_repcheckindex(mappedreplica2)  
              else:
                 if(flag_rep2_alive):
                   print("In mapped replica 2, data in replica 1 found to be corrupted,mappedserver dead")
                   print(mappedreplica2)
                   newblock=getdata_replica(mappedreplica2,path)
                   if calc_checksum(newblock)==getchecksum_replica(mappedreplica2,path):
                      text.append(newblock)
                      print("Text is")
                      print(text) 
            else:
              if(flag_rep2_alive):
                print("In mapped replica 2 because replica 1 is dead")
                print(mappedreplica2)
                newblock=getdata_replica(mappedreplica2,path)
                if calc_checksum(newblock)==getchecksum_replica(mappedreplica2,path):
                    text.append(newblock)
                    print("Text as in mapped replica 2")
                    print(text)
 
         servernum=(servernum+1)%snum
         replicanum1=(replicanum1+1)%snum
         replicanum2=(replicanum2+1)%snum
         blocks = blocks-1
 
        newstring=self.listtostring(text)
        return newstring[offset:offset + size] #tell

    def readdir(self, path, fh):
        list1= ['.', '..']
        x = getdirectory(metaserver)
        for key in x:
           if x[key]==path:
              print('x is', x)
              list1.append(os.path.basename(key))
        return list1
       

         

#read all the data from the file as string by converting the list to string
    def readlink(self, path):
        return self.listtostring(getdata(dataserver,path))
        

    def removexattr(self, path, name):
        attrs = getdata_file(metaserver,path).get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        files =getfile(metaserver)
        files[new] = files.pop(old)
        putfile(metaserver,files)
	print("new is" + new)
        print ("old is " + old)
        direct = getdirectory(metaserver)
 	direct.pop(old)
        parent=os.path.dirname(new)
	direct[new]=parent
        putdirectory(metaserver,direct)
        for server in servlist[:snum]:
          if(isalive(server) and os.path.isfile(old)):
            data = getdatafull(server)
	    data[new] = data.pop(old)
            putdatafull(server,data)
            data_rep = getdatafull_replica(server)
	    data_rep[new] = data_rep.pop(old)
            putdatafull_replica(server,data_rep)
    
    def rmdir(self, path):
        flag_empty=1
        parent = os.path.dirname(path)
        directory=getdirectory(metaserver)
        files=getfile(metaserver)
        if(os.path.isdir(path)):
           for key in directory:
               if(directory[key]==path):
                  raise FuseOSError(ENOTEMPTY)
        files.pop(path)
        files[parent]['st_nlink'] -= 1
        directory.pop(path)
        putdirectory(metaserver,directory)
        putfile(metaserver,files)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source): #target - path source-string
        self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))

        self.data[target] = self.stringtolist(source)
	self.directory[target]=self.directory[source]
	
#truncate data as required and save in form of list
    def truncate(self, path, length, fh=None):
      
########################################################################################
      if arealive():
        
        #return newstring[offset:offset + size] #tell
        for server in servlist[:snum]:
            if(isalive(server)):
              resetindex(server)
              reset_rindex(server)
              reset_checksum_index(server)
              reset_replica_checksum_index(server)
 
  #get dictionary containing metadata for the file to be read
        files=getdata_file(metaserver,path)
    
    #get the size of the data in the file
        size=files['st_size']
        print("Size of file being read")
        print(size)
        
    #get number of blocks of file data
        if((size%8) ==0):
          blocks = size/8
        else:
          blocks = math.floor((size/8))+1
        print("Number of blocks in file")
        print(blocks)

    #get starting server for the path
        servernum = getserver(path)
        replicanum1= (servernum+1)%snum
        replicanum2= (replicanum1+1)%snum
        text=[]
    #set a flag to say that checksum matched
        
        flag_alive=True
        flag_rep1_alive=True
        flag_rep2_alive=True

        while(blocks>0):
        
         mappedserver=mapserver(servernum)
         mappedreplica1=mapserver(replicanum1)
         mappedreplica2=mapserver(replicanum2)
         print(mappedserver)
         flag_alive=isalive(mappedserver) 
         flag_rep1_alive=isalive(mappedreplica1)
         flag_rep2_alive=isalive(mappedreplica2)

         if(flag_alive):
           print("Mapped server is alive")
           print(mappedserver)
           newblock=getdata(mappedserver,path)
           if calc_checksum(newblock)==getchecksum(mappedserver,path):
              print("Authentic data found in mapped server")
              text.append(newblock)
              if(isalive(mappedreplica1)):
                 increment_repindex(mappedreplica1)
                 increment_repcheckindex(mappedreplica1)
              if(isalive(mappedreplica2)):
                 increment_repindex(mappedreplica2)
                 increment_repcheckindex(mappedreplica2) 
              print("In mappedserver text is")
              print(text) 
           else:
              print("Data in mapped server found corrupted,retrieving data from replica")
              if(flag_rep1_alive):
                    print("In mapped replica 1")
                    print(mappedreplica1)
                    newblock=getdata_replica(mappedreplica1,path)
                    if calc_checksum(newblock)==getchecksum_replica(mappedreplica1,path):
                       print("Authentic data found in replica 1")
                       text.append(newblock)
                       if(isalive(mappedreplica2)):
                         increment_repindex(mappedreplica2)
                         increment_repcheckindex(mappedreplica2)
                    else:
                       if(flag_rep2_alive):
                         print("Data in replica 1 corrupted, checking in replica 2")
                         print("In mapped replica 2")
                         print(mappedreplica2)
                         newblock=getdata_replica(mappedreplica2,path)
                         if calc_checksum(newblock)==getchecksum_replica(mappedreplica2,path):
                             text.append(newblock)
                             print("In replica 2 after corruption, Text is")
                             print(text)        
         else:
            if(flag_rep1_alive):
              print("In mapped replica 1 because mappedserver dead")
              print(mappedreplica1)
              newblock=getdata_replica(mappedreplica1,path)
              if calc_checksum(newblock)==getchecksum_replica(mappedreplica1,path):
                 print("Authentic data found in replica 1,mappedserver dead")
                 print("Text is now")
                 print(text)
                 text.append(newblock)
                 if(isalive(mappedreplica2)):
                   increment_repindex(mappedreplica2)
                   increment_repcheckindex(mappedreplica2)  
              else:
                 if(flag_rep2_alive):
                   print("In mapped replica 2, data in replica 1 found to be corrupted,mappedserver dead")
                   print(mappedreplica2)
                   newblock=getdata_replica(mappedreplica2,path)
                   if calc_checksum(newblock)==getchecksum_replica(mappedreplica2,path):
                      text.append(newblock)
                      print("Text is")
                      print(text) 
            else:
              if(flag_rep2_alive):
                print("In mapped replica 2 because replica 1 is dead")
                print(mappedreplica2)
                newblock=getdata_replica(mappedreplica2,path)
                if calc_checksum(newblock)==getchecksum_replica(mappedreplica2,path):
                    text.append(newblock)
                    print("Text as in mapped replica 2")
                    print(text)
 
         servernum=(servernum+1)%snum
         replicanum1=(replicanum1+1)%snum
         replicanum2=(replicanum2+1)%snum
         blocks = blocks-1
 
         
        originaldata=self.listtostring(text)
        
        if(length <= size):
          newdata=originaldata[:length]
        else:
          newdata=originaldata
          extra=length-size
          while extra>0:
             newdata=newdata+'\0'
             extra=extra-1
            
          

        #return newstring[offset:offset + size] #tell
        for server in servlist[:snum]:
          if(isalive(server)):
            resetindex(server)
            reset_rindex(server)
            reset_checksum_index(server)  
            reset_replica_checksum_index(server)
            empty(server,path)
            emptyreplica(server,path)
            empty_checksum(server,path)
            empty_checksum_replica(server,path)
        



        #get metadata, update it and update on server
        originalmeta=getdata_file(metaserver,path)
        originalmeta['st_size'] = length
        originalmeta['st_mtime']=time()
        putdata_file(metaserver,path,originalmeta)
      

        #convert new data to list
        towrite = self.stringtolist(newdata)
       

        #get starting server number
        servernum = getserver(path)
        print("Hash gave servernumber :")
        print(servernum)
        
        #get replica server numbers
        replicaserver1 = (servernum+1)%snum
        replicaserver2 = (servernum+2)%snum
        print("Hash gave replica servernumbers :")
        print(replicaserver1)
        print(replicaserver2)
        

        #write each block in list to the mapped server
        for block in towrite:
         checksum=calc_checksum(block)
         mappedserver=mapserver(servernum)
         mappedreplica1=mapserver(replicaserver1)
         mappedreplica2=mapserver(replicaserver2)
         print("Mapped servers are:")
         print(mappedserver)
         print(mappedreplica1)
         print(mappedreplica2)
         putdata(mappedserver,path,block)
         putchecksum(mappedserver,path,checksum)
         putdata_replica(mappedreplica1,path,block)
         putchecksum_replica(mappedreplica1,path,checksum)
         putdata_replica(mappedreplica2,path,block)
         putchecksum_replica(mappedreplica2,path,checksum)
         servernum=(servernum+1)%snum
         replicaserver1=(replicaserver1+1)%snum
         replicaserver2=(replicaserver2+1)%snum
         print("Wrote to server : " + block)
         return len(newdata)

      else:
         print("All servers not available")

      

 
    def unlink(self, path):
        directory=getdirectory(metaserver)
        files=getfile(metaserver)
        files.pop(path)
        directory.pop(path)
        putdirectory(metaserver,directory)
        putfile(metaserver,files)
        for serv in servlist[:snum]:
           if(isalive(serv) and os.path.isfile(path)):
              data=getdatafull(serv)  
              data.pop(path)
              putdatafull(serv,data)
              datareplica=getdatafull_replica(serv)  
              datareplica.pop(path)
              putdatafull_replica(serv,datareplica)  

    def utimens(self, path, times=None):
        now = time()
        files =getdata_file(metaserver,path)
        atime, mtime = times if times else (now, now)
        files['st_atime'] = atime
        files['st_mtime'] = mtime
        putdata_file(metaserver,path,files)

#write data in form of list
    def write(self, path, data, offset, fh):
        
      if(arealive()): 
        
        for server in servlist[:snum]:
            if(isalive(server)):
              resetindex(server)
              reset_rindex(server)
              reset_checksum_index(server)
              reset_replica_checksum_index(server)
 
  #get dictionary containing metadata for the file to be read
        files=getdata_file(metaserver,path)
    
    #get the size of the data in the file
        size=files['st_size']
        print("Size of file being read")
        print(size)
        
    #get number of blocks of file data
        if((size%8) ==0):
          blocks = size/8
        else:
          blocks = math.floor((size/8))+1
        print("Number of blocks in file")
        print(blocks)

    #get starting server for the path
        servernum = getserver(path)
        replicanum1= servernum+1
        replicanum2= replicanum1+1
        text=[]
    #set a flag to say that checksum matched
        
        flag_alive=True
        flag_rep1_alive=True
        flag_rep2_alive=True

        while(blocks>0):
        
         mappedserver=mapserver(servernum)
         mappedreplica1=mapserver(replicanum1)
         mappedreplica2=mapserver(replicanum2)
         print(mappedserver)
         flag_alive=isalive(mappedserver) 
         flag_rep1_alive=isalive(mappedreplica1)
         flag_rep2_alive=isalive(mappedreplica2)

         if(flag_alive):
           print("Mapped server is alive")
           print(mappedserver)
           newblock=getdata(mappedserver,path)
           if calc_checksum(newblock)==getchecksum(mappedserver,path):
              print("Authentic data found in mapped server")
              text.append(newblock)
              if(isalive(mappedreplica1)):
                 increment_repindex(mappedreplica1)
                 increment_repcheckindex(mappedreplica1)
              if(isalive(mappedreplica2)):
                 increment_repindex(mappedreplica2)
                 increment_repcheckindex(mappedreplica2) 
              print("In mappedserver text is")
              print(text) 
           else:
              print("Data in mapped server found corrupted,retrieving data from replica")
              if(flag_rep1_alive):
                    print("In mapped replica 1")
                    print(mappedreplica1)
                    newblock=getdata_replica(mappedreplica1,path)
                    if calc_checksum(newblock)==getchecksum_replica(mappedreplica1,path):
                       print("Authentic data found in replica 1")
                       text.append(newblock)
                       if(isalive(mappedreplica2)):
                         increment_repindex(mappedreplica2)
                         increment_repcheckindex(mappedreplica2)
                    else:
                       if(flag_rep2_alive):
                         print("Data in replica 1 corrupted, checking in replica 2")
                         print("In mapped replica 2")
                         print(mappedreplica2)
                         newblock=getdata_replica(mappedreplica2,path)
                         if calc_checksum(newblock)==getchecksum_replica(mappedreplica2,path):
                             text.append(newblock)
                             print("In replica 2 after corruption, Text is")
                             print(text)        
         else:
            if(flag_rep1_alive):
              print("In mapped replica 1 because mappedserver dead")
              print(mappedreplica1)
              newblock=getdata_replica(mappedreplica1,path)
              if calc_checksum(newblock)==getchecksum_replica(mappedreplica1,path):
                 print("Authentic data found in replica 1,mappedserver dead")
                 print("Text is now")
                 print(text)
                 text.append(newblock)
                 if(isalive(mappedreplica2)):
                   increment_repindex(mappedreplica2)
                   increment_repcheckindex(mappedreplica2)  
              else:
                 if(flag_rep2_alive):
                   print("In mapped replica 2, data in replica 1 found to be corrupted,mappedserver dead")
                   print(mappedreplica2)
                   newblock=getdata_replica(mappedreplica2,path)
                   if calc_checksum(newblock)==getchecksum_replica(mappedreplica2,path):
                      text.append(newblock)
                      print("Text is")
                      print(text) 
            else:
              if(flag_rep2_alive):
                print("In mapped replica 2 because replica 1 is dead")
                print(mappedreplica2)
                newblock=getdata_replica(mappedreplica2,path)
                if calc_checksum(newblock)==getchecksum_replica(mappedreplica2,path):
                    text.append(newblock)
                    print("Text as in mapped replica 2")
                    print(text)
 
         servernum=(servernum+1)%snum
         replicanum1=(replicanum1+1)%snum
         replicanum2=(replicanum2+1)%snum
         blocks = blocks-1
 
         
        originaldata=self.listtostring(text)
        newdata=originaldata[:offset]+data 

        #return newstring[offset:offset + size] #tell
        for server in servlist[:snum]:
          if(isalive(server)):
            resetindex(server)
            reset_rindex(server)
            reset_checksum_index(server)  
            reset_replica_checksum_index(server)
        
        

        #clear dataserver
        for server in servlist[:snum]:
          if(isalive(server)):
            empty(server,path)
            emptyreplica(server,path)
            empty_checksum(server,path)
            empty_checksum_replica(server,path)
       

        #get metadata, update it and update on server
        originalmeta=getdata_file(metaserver,path)
        originalmeta['st_size'] = len(newdata)
        originalmeta['st_mtime']=time()
        putdata_file(metaserver,path,originalmeta)
      

        #convert new data to list
        towrite = self.stringtolist(newdata)
       
        #get starting server number
        servernum = getserver(path)
        print("Hash gave servernumber :")
        print(servernum)
        
        #get replica server numbers
        replicaserver1 = (servernum+1)%snum
        replicaserver2 = (servernum+2)%snum
        print("Hash gave replica servernumbers :")
        print(replicaserver1)
        print(replicaserver2)
        

        #write each block in list to the mapped server
        for block in towrite:
         checksum=calc_checksum(block)
         mappedserver=mapserver(servernum)
         mappedreplica1=mapserver(replicaserver1)
         mappedreplica2=mapserver(replicaserver2)
         print("Mapped servers are:")
         print(mappedserver)
         print(mappedreplica1)
         print(mappedreplica2)
         putdata(mappedserver,path,block)
         putchecksum(mappedserver,path,checksum)
         putdata_replica(mappedreplica1,path,block)
         putchecksum_replica(mappedreplica1,path,checksum)
         putdata_replica(mappedreplica2,path,block)
         putchecksum_replica(mappedreplica2,path,checksum)
         servernum=(servernum+1)%snum
         replicaserver1=(replicaserver1+1)%snum
         replicaserver2=(replicaserver2+1)%snum
         print("Wrote to server : " + block)

        return len(data)
      else:
        print("All servers not available")

#method to convert string to list such that each element of list is 8 bytes
    def stringtolist(self,str1):
        final=[]
        while str1:
            final.append(str1[:8])
            str1=str1[8:]
        return final
 
#method to convert list to string such that a meaningful string is obtained  
    def listtostring(self,list1):
        str1=''.join(list1)
        return str1

        

if __name__ == '__main__':
    if len(argv) <= 7:
        print('usage: %s <mountpoint> <metaserver_port> <dataserver_port>' % argv[0])
        exit(1)
    dataport0=argv[3]
    dataport1=argv[4]
    dataport2=argv[5]
    dataport3=argv[6]
    dataport4=argv[7]
    metaport=argv[2]
    dataserver0 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport0)))
    dataserver1 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport1)))
    dataserver2 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport2)))
    dataserver3 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport3)))
    dataserver4 = xmlrpclib.ServerProxy("http://localhost:"+str(int(dataport4)))
    snum=len(argv)-3
    servlist= [dataserver0,dataserver1,dataserver2,dataserver3,dataserver4]
    metaserver = xmlrpclib.ServerProxy("http://localhost:"+str(int(metaport)))
    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True,debug=True)
