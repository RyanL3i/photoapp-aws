#
# Main program for photoapp program using AWS S3 and RDS to
# implement a simple photo application for photo storage and
# viewing.
#
# Authors:
#   Ryan Lei
#   Prof. Joe Hummel (initial template)
#   Northwestern University
#

import datatier  # MySQL database access
import awsutil  # helper functions for AWS
import boto3  # Amazon AWS

import uuid
import pathlib
import logging
import sys
import os

from configparser import ConfigParser

import matplotlib.pyplot as plt
import matplotlib.image as img


###################################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number
  
  Parameters
  ----------
  None
  
  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """

  try:
    print()
    print(">> Enter a command:")
    print("   0 => end")
    print("   1 => stats")
    print("   2 => users")
    print("   3 => assets")
    print("   4 => download")
    print("   5 => download and display")
    print("   6 => upload")
    print("   7 => add user")

    cmd = int(input())
    return cmd

  except Exception as e:
    print("ERROR")
    print("ERROR: invalid input")
    print("ERROR")
    return -1


###################################################################
#
# stats
#
def stats(bucketname, bucket, endpoint, dbConn):
  """
  Prints out S3 and RDS info: bucket name, # of assets, RDS 
  endpoint, and # of users and assets in the database
  
  Parameters
  ----------
  bucketname: S3 bucket name,
  bucket: S3 boto bucket object,
  endpoint: RDS machine name,
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try: 
    print("S3 bucket name:", bucketname)

    assets = bucket.objects.all()
    print("S3 assets:", len(list(assets)))

    #
    # MySQL info:
    #
    print("RDS MySQL endpoint:", endpoint)
    
    sql_users = "SELECT COUNT(*) FROM users;"
    row_users = datatier.retrieve_one_row(dbConn, sql_users)
    print("# of users:", row_users[0])
    
    sql_assets = "SELECT COUNT(*) FROM assets;"
    row_assets = datatier.retrieve_one_row(dbConn, sql_assets)
    
    print('# of assets:', row_assets[0] )

    # sql = """
    # select now();
    # """

    # row = datatier.retrieve_one_row(dbConn, sql)
    # if row is None:
    #   print("Database operation failed...")
    # elif row == ():
    #   print("Unexpected query failure...")
    # else:
    #   print(row[0])

  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))

# users function   
def users(dbConn):
  """
  prints out user ID, email, their name, and the folder ID that belongs to them
  
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  try:
    sql = """SELECT userid, email, lastname, firstname, bucketfolder 
            FROM users
            ORDER BY userid DESC;"""
            
    rows = datatier.retrieve_all_rows(dbConn, sql)
    
    if not rows:
      print("No data found")
    
    for row in rows:
      userid, email, lastname, firstname, bucketfolder = row
      print(f"User id: {userid}")
      print(f"  Email: {email}")
      print(f"  Name: {lastname} , {firstname}")
      print(f"  Folder: {bucketfolder}")
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))
  

# assets function
def assets(dbConn):
  """
  prints out assets ID, the userID who owns the asset, the asset's original name, and the bucket id / asset id for identification
  
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  try:
    sql = """SELECT assetid, userid, assetname, bucketkey
            FROM assets
            ORDER BY assetid DESC;"""
            
    rows = datatier.retrieve_all_rows(dbConn, sql)
    
    if not rows:
      print("No data found")
    
    for row in rows:
      assetid, userid, assetname, bucketkey = row
      print(f"Asset id: {assetid}")
      print(f"  User id: {userid}")
      print(f"  Original Name: {assetname}")
      print(f"  Key Name: {bucketkey}")
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))
    
# download function
def download(dbConn, bucket, command):
  """
  Looks up the asset by asset_id, downloads the file, while renaming the download to the original filename
  Also has the capability of displaying the image, which can be done through the "command" parameter
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  bucket: S3 boto bucket object
  asset_id: the ID of the asset to download
  command: if 5, will display the image besides just downloading it
  
  Returns
  -------
  nothing
  """
  try:
    search = input("Enter asset id> \n")
    
    search = int(search)
    
    sql = """
    SELECT assetid, assetname, bucketkey 
    FROM assets
    WHERE assetid = %s;
    """
    
    row = datatier.retrieve_one_row(dbConn, sql, [search])
    
    if len(row) == 0 or not row:
      print("No such asset...")
      return
    
    assetid, name, assetkey = row
    
    download_name = awsutil.download_file(bucket, assetkey)
    
    if not download_name:
      print("Failed to download file...")
      
    pathlib.Path(download_name).rename(name)
    print(f"Downloaded from S3 and saved as ' {name} '")
    
    # for command #5
    
    if command == 5:
      image = img.imread(name)
      plt.imshow(image)
      plt.show()
    
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))
    
# upload function
def upload(dbConn, bucket):
  """
  Uploads a local file to bucket and records the asset details in the RDS database.
  The asset ID is assigned as the highest asset ID currently in the database, + 1.

  Parameters
  ----------
  dbConn : open connection to MySQL server
  bucket : S3 boto bucket object

  Returns
  -------
  None
  """
  
  try:
    upload =  input("Enter local filename> ").strip()
    user_id = input("Enter user id> ").strip()
    
    if not os.path.isfile(upload):
      print(f"Local file ' {upload} ' does not exist...")
      return
    
    sql_user = """
    SELECT userid, bucketfolder
    FROM users
    WHERE userid = %s;
    """
    
    row = datatier.retrieve_one_row(dbConn, sql_user, [user_id])
    
    if not row:
      print("No user found...")
      return
    folder = str(row[1])
    
    asset_key = folder + '/' + str(uuid.uuid4()) + ".jpg"

    

    key_uploaded = awsutil.upload_file(upload, bucket, asset_key)
    
    if not key_uploaded:
      print("Error uploading....")
      return
    
    print(f"Uploaded and stored in S3 as '{key_uploaded}'")
    
    # gets the current biggest asset id, so we have a new ID to assign it to
    sql_curr_max = """
    SELECT MAX(assetid) FROM assets
    """
    row = datatier.retrieve_one_row(dbConn, sql_curr_max)
    next_id = row[0] + 1
    
    # insert into table database
    
    sql_insert = """
    INSERT INTO assets (assetid, userid, assetname, bucketkey)
    VALUES (%s, %s, %s, %s);
    """
    
    rows_affected = datatier.perform_action(dbConn, sql_insert, [next_id, user_id, upload, key_uploaded])
    
    if rows_affected < 1:
      print("Error inserting into database")
      
    print(f"Recorded in RDS under asset id {next_id}")
    
    
     
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))
    
  
def adduser(dbConn):
    """
    Uploads a user and records user details in the RDS database.
    The user ID is assigned as the highest user ID currently in the database, + 1.

    Parameters
    ----------
    dbConn : open connection to MySQL server

    Returns
    -------
    None
    """
    email = input("Enter user's email>")
    last = input("Enter user's last (family) name>")
    first = input("Enter user's first (given) name>")
    folder = str(uuid.uuid4())
    
    # gets the current biggest user id, so we have a new ID to assign it to
    sql_curr_max = """
    SELECT MAX(userid) FROM users
    """
    row = datatier.retrieve_one_row(dbConn, sql_curr_max)
    next_id = row[0] + 1
    
    
    sql_insert = """
    INSERT INTO users (userid, email, lastname, firstname, bucketfolder)
    VALUES (%s, %s, %s, %s, %s);
    """
    
    rows_affected = datatier.perform_action(dbConn, sql_insert, [next_id, email, last, first, folder])
    
    if rows_affected < 1:
      print("Error inserting into database")
      
    print(f"Recorded in RDS under user id {next_id}")
      
    
    
    
  
  
    

#########################################################################
# main
#
print('** Welcome to PhotoApp **')
print()

# eliminate traceback so we just get error message:
sys.tracebacklimit = 0

#
# what config file should we use for this session?
#
config_file = 'photoapp-config.ini'

print("What config file to use for this session?")
print("Press ENTER to use default (photoapp-config.ini),")
print("otherwise enter name of config file>")
s = input()

if s == "":  # use default
  pass  # already set
else:
  config_file = s

#
# does config file exist?
#
if not pathlib.Path(config_file).is_file():
  print("**ERROR: config file '", config_file, "' does not exist, exiting")
  sys.exit(0)

#
# gain access to our S3 bucket:
#
s3_profile = 's3readwrite'

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file

boto3.setup_default_session(profile_name=s3_profile)

configur = ConfigParser()
configur.read(config_file)
bucketname = configur.get('s3', 'bucket_name')

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucketname)

#
# now let's connect to our RDS MySQL server:
#
endpoint = configur.get('rds', 'endpoint')
portnum = int(configur.get('rds', 'port_number'))
username = configur.get('rds', 'user_name')
pwd = configur.get('rds', 'user_pwd')
dbname = configur.get('rds', 'db_name')

dbConn = datatier.get_dbConn(endpoint, portnum, username, pwd, dbname)

if dbConn is None:
  print('**ERROR: unable to connect to database, exiting')
  sys.exit(0)

#
# main processing loop:
#
cmd = prompt()

while cmd != 0:
  #
  if cmd == 1:
    stats(bucketname, bucket, endpoint, dbConn)
  
  elif cmd == 2:
    users(dbConn)
    
  elif cmd == 3:
    assets(dbConn)
    
  elif cmd == 4:
    download(dbConn, bucket, 4)
    
  elif cmd == 5:
    download(dbConn, bucket, 5)
    
  elif cmd == 6:
    upload(dbConn, bucket)
    
  elif cmd == 7:
    adduser(dbConn)
  #
  #
  # TODO
  ###################################################################
  #
  # stats
  #
  
  #
  else:
    print("** Unknown command, try again...")
  #
  cmd = prompt()

#
# done
#
print()
print('** done **')
