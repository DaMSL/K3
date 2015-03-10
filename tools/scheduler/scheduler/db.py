import psycopg2
import datetime as dt
from pytz import timezone
import sys


# Helper functions for Time Stamps
TS_FMT = "%Y-%m-%d %H:%M:%S %Z%z"
def getTS_utc():
    return dt.datetime.now(timezone('UTC')).strftime(TS_FMT)

def getTS_est():
    return dt.datetime.now(timezone('US/Eastern')).strftime(TS_FMT)


# ts = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def getConnection():
  conn = psycopg2.connect("host=qp1 dbname=postgres user=postgres password=password")
  return conn


tables = {
  'app': '''
CREATE TABLE IF NOT EXISTS apps (
    name        text,
    latest      text
);''',
  'app_versions': '''
CREATE TABLE IF NOT EXISTS app_versions (
    hash        text,
    appname     text,
    date        date
);''',
  'jobs': '''
CREATE TABLE IF NOT EXISTS jobs (
    jobId       integer,
    appId       text,
    app_version text,
    username    text,
    time        date,
    status      text
);'''}


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


def insertApp(name, version):
  conn = getConnection()
  cur = conn.cursor()
  cur.execute("SELECT COUNT(*) FROM apps WHERE name='%s';" % name)
  if int(cur.fetchone()[0]) == 0:
    cur.execute("INSERT INTO apps VALUES ('%s', '%s');" % (name, version))
    conn.commit()
  cur.execute("INSERT INTO app_versions VALUES ('%s', '%s', '%s');" % (version, name, str(dt.date.today())))
  conn.commit()


def getApp(appId):
  cur = getConnection().cursor()
  cur.execute(
    "SELECT a.name, v.hash, v.date FROM apps AS a, app_versions as V WHERE a.name=v.appname AND a.latest=v.hash AND a.name='%s';" % appId)
  r = cur.fetchone()[0]
  return dict(name=r[0], hash=r[1], date=r[2])


def getAllApps():
  cur = getConnection().cursor()
  cur.execute(
    "SELECT a.name, v.hash, v.date FROM apps AS a, app_versions as V WHERE a.name=v.appname AND a.latest=v.hash;")
  return [dict(name=r[0], hash=r[1], date=r[2]) for r in cur.fetchall()]


def checkHash(hash):
  cur = getConnection().cursor()
  query = "SELECT COUNT(*) FROM app_versions WHERE hash='%s';" % hash
  print query
  cur.execute(query)
  return False if int(cur.fetchone()[0]) == 0 else True


def insertJob(job):
  conn = getConnection()
  cur = conn.cursor()
  cur.execute("SELECT MAX(jobId) FROM jobs;")
  result = cur.fetchone()
  jobId = 1000 if result[0] == None else int(result[0]) + 1
  time = str(dt.date.today())
  cur.execute("INSERT INTO jobs VALUES ('%(jobId)d', '%(appId)s', '%(hash)s', '%(user)s', '%(time)s', '%(status)s');" %
              dict(job, jobId=jobId, status='SUBMITTED', time=time))
  conn.commit()
  return jobId, time


def getJobs(**kwargs):
  appId = kwargs.get('appId', None)
  jobId = kwargs.get('jobId', None)
  filter = ("" if not appId and not jobId
            else (" WHERE appId='%s'" % appId if appId
                  else " WHERE jobId='%s'" % jobId))
  cur = getConnection().cursor()
  cur.execute("SELECT * from jobs " + filter + " ORDER BY jobID DESC;")
  return [dict(jobId=r[0], appId=r[1], hash=r[2], user=r[3], time=r[4], status=r[5]) for r in cur.fetchall()]


def updateJob(jobId, **kwargs):
  status = kwargs.get("status", None)
  conn = getConnection()
  cur = conn.cursor()

  if status:
    cur.execute("UPDATE jobs SET status = '%s' WHERE jobId='%s';" % (status, jobId))
    conn.commit()
