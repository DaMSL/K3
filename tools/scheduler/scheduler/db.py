#import psycopg2
import sqlite3
import datetime as dt
from pytz import timezone
import sys
import os

SQLITE_DB = 'data.db'

#  Load Postgres Connection Data from enviornment (or load defaults)
# HOST    = os.environ.get('PGHOST',      'localhost')
# DBNAME  = os.environ.get('PGDATABASE',  'postgres')
# USER    = os.environ.get('PGUSER',      'postgres')
# PASSWD  = os.environ.get('PGPASSWORD',  'password')


# Helper functions for Time Stamps
TS_FMT = "%Y-%m-%d %H:%M:%S"
def getTS_utc():
    return dt.datetime.now(timezone('UTC')).strftime(TS_FMT)

def getTS_est(ts=None):
    if ts:
      time = dt.datetime.strptime(ts, TS_FMT).replace(tzinfo=timezone('UTC'))
      return time.astimezone(timezone('US/Eastern')).strftime(TS_FMT)
    else:
      return dt.datetime.now(timezone('US/Eastern')).strftime(TS_FMT)



#  Connection String
def getConnection():
#  conn = psycopg2.connect(host=HOST, dbname=DNAME, user=USER, password=PASSWD)
  conn = sqlite3.connect(SQLITE_DB)
  return conn


tables = {
  'app': '''
CREATE TABLE IF NOT EXISTS apps (
    name        text,
    latest      text
);''',
  'app_versions': '''
CREATE TABLE IF NOT EXISTS app_versions (
    uid         text,
    name        text,
    hash        text,
    path        text,
    archive     text,
    date        timestamp
);''',
  'jobs': '''
CREATE TABLE IF NOT EXISTS jobs (
    jobId       integer,
    appName     name,
    appUID      text,
    username    text,
    submit      timestamp,
    complete    timestamp,
    tag         text,
    status      text
);''',
  'compiles': '''
CREATE TABLE IF NOT EXISTS compiles (
    name        text,
    uid         text,
    git_hash    text,
    username    text,
    options     text,
    submit      timestamp,
    complete    timestamp,
    tag         text,
    status      text
);'''
}


def createTables():
  conn = getConnection()
  try:
    cur = conn.cursor()
    for table, query in tables.items():
      cur.execute(query)
      conn.commit()
  except Exception as ex:
    print("Failed to create tables:" )
    print(ex)
    sys.exit(1)

def dropTables():
  conn = getConnection()
  try:
    cur = conn.cursor()
    for table in tables.keys():
      query = "DROP TABLE IF EXISTS %s CASCADE;" % table
      cur.execute(query)
      conn.commit()
  except Exception as ex:
    print("Failed to drop tables:" )
    print(ex)
    sys.exit(1)

def insertApp(app):  #name, version):
  conn = getConnection()
  cur = conn.cursor()
  cur.execute("SELECT COUNT(*) FROM apps WHERE name='%s';" % app['name'])
  if int(cur.fetchone()[0]) == 0:
    cur.execute("INSERT INTO apps VALUES ('%(name)s', '%(uid)s');" % (app))
  else:
    cur.execute("UPDATE apps SET latest='%(uid)s' WHERE name='%(name)s'" % (app))
  conn.commit()
  time = str(getTS_utc())
  cur.execute('''
INSERT INTO app_versions (uid, name, hash, date)
VALUES ('%(uid)s', '%(name)s', '%(hash)s', '%(time)s');''' % dict(app, time=time))
  conn.commit()

def getApp(appName, appUID=None):
  cur = getConnection().cursor()
  if appUID:
    cur.execute(
       "SELECT name, uid, date FROM app_versions WHERE name='%s' AND uid='%s';" % (appName, appUID))
  else:
    cur.execute(
      "SELECT a.name, v.uid, v.date FROM apps AS a, app_versions as V WHERE a.name=v.name AND a.latest=v.uid AND a.name='%s';" % appName)
  r = cur.fetchone()
  return dict(name=r[0], uid=r[1], date=r[2])

def getAllApps(appName=None):
  cur = getConnection().cursor()
  if appName:
    cur.execute("SELECT name, uid, date FROM app_versions WHERE name='%s';" % appName)
  else:
    cur.execute(
      "SELECT a.name, v.uid, v.date FROM apps AS a, app_versions AS v WHERE a.name=v.name AND a.latest=v.uid;")
  return [dict(name=r[0], uid=r[1], date=r[2]) for r in cur.fetchall()]

def getVersions(appName, limit=None):
    cur = getConnection().cursor()
    limitRows = '' if not limit else 'LIMIT %d' % limit
    cur.execute('''
SELECT name, uid, name || '-' || uid as version, date, git_hash, username, options
FROM app_versions LEFT OUTER JOIN compiles USING (name, uid) WHERE name='%s' ORDER BY date DESC %s;
''' % (appName, limitRows))
    return [dict(name=r[0], uid=r[1], version=r[2], date=r[3], git_hash=r[4], user=r[5], options=r[6]) for r in cur.fetchall()]

def checkHash(hash):
  cur = getConnection().cursor()
  cur.execute("SELECT COUNT(*) FROM app_versions WHERE hash='%s';" % hash)
  return False if int(cur.fetchone()[0]) == 0 else True

def insertJob(job):
  conn = getConnection()
  cur = conn.cursor()
  cur.execute("SELECT MAX(jobId) FROM jobs;")
  result = cur.fetchone()
  jobId = 1000 if result[0] == None else int(result[0]) + 1
  time = str(getTS_utc())
  cur.execute('''
INSERT INTO jobs (jobId, appName, appUID, username, submit, tag, status)
VALUES ('%(jobId)d', '%(appName)s', '%(appUID)s', '%(user)s', '%(time)s', '%(tag)s', '%(status)s');
''' %  dict(job, jobId=jobId, status='SUBMITTED', time=time))
  conn.commit()
  return jobId, getTS_est(time)

def getJobs(**kwargs):
  appName = kwargs.get('appName', None)
  jobId = kwargs.get('jobId', None)
  filter = ("" if not appName and not jobId
            else (" WHERE appName='%s'" % appName if appName
                  else " WHERE jobId='%s'" % jobId))
  cur = getConnection().cursor()
  cur.execute("SELECT * from jobs " + filter + " ORDER BY jobID DESC;")
  return [dict(jobId=r[0], appName=r[1], hash=r[2], user=r[3],
               time=r[4], complete=r[5], tag=r[6], status=r[7]) for r in cur.fetchall()]

def updateJob(jobId, **kwargs):
  status = kwargs.get("status", None)
  done = kwargs.get("done", False)
  conn = getConnection()
  cur = conn.cursor()

  complete = ", complete='%s'" % str(getTS_utc()) if done else ''
  if status:
    cur.execute("UPDATE jobs SET status='%s'%s WHERE jobId=%s;" % (status, complete, jobId))
    conn.commit()

def deleteJob(jobId):
  conn = getConnection()
  cur = conn.cursor()
  cur.execute("DELETE FROM jobs WHERE jobId=%s;" % jobId)
  conn.commit()

def insertCompile(comp):
  conn = getConnection()
  cur = conn.cursor()
  time = str(getTS_utc())
  cur.execute('''
INSERT INTO compiles (name, uid, git_hash, username, options, submit, tag, status)
VALUES('%(name)s', '%(uid)s', '%(git_hash)s', '%(user)s', '%(options)s', '%(time)s', '%(tag)s', '%(status)s');
''' % dict(comp, time=time, status='SUBMITTED'))
  conn.commit()
  return getTS_est(time)

def updateCompile(uid, **kwargs):
  status    = kwargs.get("status", None)
  done      = kwargs.get("done", False)
  conn = getConnection()
  cur = conn.cursor()

  if status:
    cur.execute("UPDATE compiles SET status='%s' WHERE uid='%s';" % (status, uid))
    conn.commit()

    if done:
      time = str(getTS_utc())
      cur.execute("UPDATE compiles SET complete='%s' WHERE uid='%s';" % (time, uid))
      conn.commit()

def getCompiles(**kwargs):
  appName = kwargs.get('appName', None)
  uid     = kwargs.get('uid', None)
  active  = kwargs.get('active', False)

  filter = ("WHERE appName='%s'" % appName if appName
              else "WHERE uid='%s'" % uid if uid
                  else "")

  cur = getConnection().cursor()
  cur.execute("SELECT * from compiles %s ORDER BY submit DESC;" % filter)
  return [dict(name=r[0], uid=r[1], git_hash=r[2], user=r[3],
               options=r[4], submit=r[5], complete=r[6],
               tag=r[7], status=r[8]) for r in cur.fetchall()]


