#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.03

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => Binary
      print rv.data => "value"
  put(base64 key, base64 value)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"))
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable

Changelog:
    0.03 - Modified to remove timeout mechanism for data.
"""
import shelve
import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from collections import defaultdict
index=0 
replica_index=0
checksum_index=0
checksum_replica_index=0
# Presents a HT interface
class SimpleHT:

  
  
  def __init__(self):
    self.data = defaultdict(list)
    self.replica=defaultdict(list)
    self.checksum=defaultdict(list)
    self.checksum_replica=defaultdict(list)
    
    
  def count(self):
    return len(self.data)

  def checkalive(self):
    return True  
  
  def emptychecksum(self,key):
    key= key.data
    global datastore
    db=shelve.open(datastore)
    try:
      self.checksum=db['checksum']
    except:
      pass
    if key in self.checksum:
    	self.checksum[key]=[] 
    db['checksum']=self.checksum
    db.close()
    return True

  def emptychecksumreplica(self,key):
    global datastore
    db=shelve.open(datastore)
    key= key.data
    try:
     self.checksum=db['checksum_replica']
    except:
      pass
    if key in self.checksum_replica:
    	self.checksum_replica[key]=[]
    db['checksum_replica']=self.checksum_replica
    db.close()
    return True
  
  def reset_checksum_index(self):
    db=shelve.open(datastore)
    global checksum_index
    try:
      checksum_index=db['checksum_index']
    except:
      pass
    checksum_index=0
    db['checksum_index']=checksum_index 
    db.close()
    return True 
   
  def reset_checksum_rindex(self):
    db=shelve.open(datastore)
    global checksum_replica_index
    try:
      checksum_replica_index=db['checksum_replica_index']
    except:
      pass
    checksum_replica_index=0 
    db['checksum_replica_index']=checksum_replica_index
    db.close()
    return True 
  
  def get_checksum(self,key):
      # Default return value
    
    global datastore
    db=shelve.open(datastore)
    rv = ''
    global checksum_index
    print(" In dataserver 1, value of checksum index now is")
    print(checksum_index)
    try:
      checksum_index=db['checksum_index']
    except:
      pass
    try:
      self.checksum=db['checksum']
    except:
      pass
    # If the key is in the data structure, return properly formated results
    key = key.data
    if key in self.checksum:
      rv=self.checksum[key][checksum_index] 
      checksum_index+=1  
    db['checksum']=self.checksum
    db['checksum_index']=checksum_index
    db.close()
    return Binary(rv) 

  def put_checksum(self,key,value): 
    # Remove expired entries
    global datastore
    db=shelve.open(datastore)
    try:
      self.checksum=db['checksum']
    except:
      pass
    self.checksum[key.data].append(value.data)
    db['checksum']=self.checksum
    db.close()
    print (" Wrote %s in server at %s key ", value.data,key.data)
    return True

  def get_checksum_replica(self,key):
      # Default return value
    global datastore
    rv = ''
    global checksum_replica_index
    db=shelve.open(datastore)
    try:
     checksum_replica_index=db['checksum_replica_index']
    except:
     pass
    try:
     self.checksum_replica=db['checksum_replica']
    except:
     pass
    print(" In dataserver 1, value of checksum replica index now is")
    print(checksum_replica_index)
    # If the key is in the data structure, return properly formated results
    key = key.data
    if key in self.checksum_replica:
      rv=self.checksum_replica[key][checksum_replica_index] 
      checksum_replica_index+=1  
    db['checksum_replica_index']=checksum_replica_index
    db.close()
    return Binary(rv) 


  def put_checksum_replica(self,key,value):
    global datastore
    # Remove expired entries
    db=shelve.open(datastore)
    try:
     self.checksum_replica=db['checksum_replica']
    except:
      pass
    self.checksum_replica[key.data].append(value.data)
    print (" Wrote %s in server at %s key ", value.data,key.data)
    db['checksum_replica']=self.checksum_replica
    db.close()
    return True

  def emptyserver(self,key):
    global datastore
    db=shelve.open(datastore)
    key= key.data
    try:
      self.data=db['data']
    except:
      pass
    if key in self.data:
    	self.data[key]=[]
    db['data']=self.data
    db.close()
    return True

  def reset(self):
    global index
    global datastore
    db=shelve.open(datastore)
    index=0 
    db['index']=index
    db.close()
    return True 

  def emptyreplica(self,key):
    global datastore
    key= key.data
    db=shelve.open(datastore)
    try:
      self.replica=db['replica']
    except:
      pass
    if key in self.replica:
    	self.replica[key]=[]
    db['replica']=self.replica
    db.close()
    return True

  def reset_replica(self):
    global replica_index
    global datastore
    db=shelve.open(datastore)
    replica_index=0 
    db['replica_index']=replica_index
    db.close()
    return True 
  
  def get_from_replica(self, key):
    # Default return value
    global datastore
    db=shelve.open(datastore)
    rv = ''
    global replica_index
    try:
      replica_index=db['replica_index']
    except:
      pass
    print(" In dataserver 0, value of index now is")
    print(replica_index)
    # If the key is in the data structure, return properly formated results
    key = key.data
    try:
     self.replica=db['replica']
    except:
      pass
    if key in self.replica:
      rv=self.replica[key][replica_index] 
      replica_index+=1  
    
    db['replica_index']=replica_index
    db.close()
    return Binary(rv) 

  # Insert something into the HT
  def put_to_replica(self, key, value):
    # Remove expired entries
    global datastore
    db=shelve.open(datastore)
    try:
      self.replica=db['replica']
    except:
      pass
    self.replica[key.data].append(value.data)
    db['replica']=self.replica
    db.close()
    print (" Wrote %s in server at %s key ", value.data,key.data)
    return True
  
  def inc_replicaindex(self):
    global replica_index
    global datastore
    db=shelve.open(datastore)
    try:
      replica_index=db['replica_index']
    except:
      pass
    replica_index = replica_index+1
    db['replica_index']=replica_index
    db.close()
    return True
  
  def inc_replicacheckindex(self):
    global checksum_replica_index
    global datastore
    db=shelve.open(datastore)
    try:
      checksum_replica_index=db['checksum_replica_index']
    except:
      pass
    checksum_replica_index = checksum_replica_index+1
    db['checksum_replica_index']=checksum_replica_index
    db.close()
    return True

# Retrieve something from the HT
  #def get(self, key):
    # Default return value
    #rv = []
    # If the key is in the data structure, return properly formated results
    #key = key.data
    #if key in self.data:
      #rv = Binary(self.data[key])
    #return rv
  
  def get(self, key):
    global datastore
  # Default return value
    rv = ''
    db=shelve.open(datastore)
    try:
      self.data=db['data']
    except:
      pass
  # If the key is in the data structure, return properly formated results
    key = key.data
    global index
    try:
      index=db['index']
    except:
      pass
    print(" In dataserver 1, value of index now is")
    print(index)
    if key in self.data:
      rv=self.data[key][index] 
      index+=1 
    db['index']=index 
    db.close()
    return Binary(rv) 


  # Insert something into the HT
  def put(self, key, value):
    # Remove expired entries
    global datastore
    db=shelve.open(datastore)
    try:
      self.data=db['data']
    except:
      pass
    self.data[key.data].append(value.data)
    db['data']=self.data
    db.close()
    print (" Wrote %s in server at %s key ", value.data,key.data)
    return True

  # Retrieve something from the HT
  def getwholedata(self):
    # Default return value
    global datastore
    db=shelve.open(datastore)
    try:
      self.data=db['data']
    except:
      pass
    rv = self.data
    db.close()
    return Binary(pickle.dumps(rv))

  # Insert something into the HT
  def putwholedata(self,value):
    # Remove expired entries
    global datastore
    db=shelve.open(datastore)
    try:
      self.data=db['data']
    except:
      pass
    self.data =pickle.loads(value.data)
    db['data']=self.data
    db.close()
    print("Now in dict")
    print(self.data)
    return True

  # Retrieve something from the HT
  def getwholedata_replica(self):
    # Default return value
    global datastore
    db=shelve.open(datastore)
    try:
      self.replica=db['replica']
    except:
      pass
    rv = self.replica
    db.close()
    return Binary(pickle.dumps(rv))

  # Insert something into the HT
  def putwholedata_replica(self,value):
    # Remove expired entries
    global datastore
    db=shelve.open(datastore)
    try:
      self.replica=db['replica']
    except:
      pass
    self.replica =pickle.loads(value.data)
    db['replica']=self.replica
    db.close()
    print("Now in dict")
    print(self.replica)
    return True

  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

def main(): 
  global datastore
  datastore='datastore'+sys.argv[1]+'.db'
  port=0
  if(sys.argv[1]=='0'):
    port=int(sys.argv[2])
  elif(sys.argv[1]=='1'):
    port=int(sys.argv[3])
  elif(sys.argv[1]=='2'):
    port=int(sys.argv[4])
  elif(sys.argv[1]=='3'):
    port=int(sys.argv[5])
  elif(sys.argv[1]=='4'):
    port=int(sys.argv[6])
  print('Port is')
  port=int(port)
  print(port)

  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.register_function(sht.getwholedata)
  file_server.register_function(sht.putwholedata)
  file_server.register_function(sht.getwholedata_replica)
  file_server.register_function(sht.putwholedata_replica)
  file_server.register_function(sht.checkalive)
  file_server.register_function(sht.emptyserver)
  file_server.register_function(sht.reset)
  file_server.register_function(sht.reset_replica)
  file_server.register_function(sht.emptyreplica)
  file_server.register_function(sht.get_from_replica)
  file_server.register_function(sht.put_to_replica)
  file_server.register_function(sht.inc_replicaindex)
  file_server.register_function(sht.inc_replicacheckindex)
  file_server.register_function(sht.emptychecksum)
  file_server.register_function(sht.emptychecksumreplica)
  file_server.register_function(sht.reset_checksum_index)
  file_server.register_function(sht.reset_checksum_rindex)
  file_server.register_function(sht.get_checksum)
  file_server.register_function(sht.get_checksum_replica)
  file_server.register_function(sht.put_checksum)
  file_server.register_function(sht.put_checksum_replica)

  
  file_server.serve_forever()
  

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(51234, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main()
