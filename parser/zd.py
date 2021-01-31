import os
from Globals import WORK_DIR
from ZODB import FileStorage, DB

storage = FileStorage.FileStorage( os.path.join( WORK_DIR,  "data", "mydatabase.fs") )
zopedb = DB(storage)
connection = zopedb.open()

root = connection.root()

print("Len", len(root["snapshot"]) )