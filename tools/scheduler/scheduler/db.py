import psycopg2
import datetime as dt
from pytz import timezone
import sys


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
    uid         text,
    name        text,
    hash        text,
    path        text,
    archive     text,
    date        date
);''',
  'jobs': '''
CREATE TABLE IF NOT EXISTS jobs (
    jobId       integer,
    appName     name,
    appUID      text,
    username    text,
    time        date,
    status      text
);''',
  'compiles': '''
CREATE TABLE IF NOT EXISTS compiles (
    name        text,
    uid         text,
    git_hash    text,
    username    text,
    options     text,
    submit      date,
    complete    date,
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

def getVersions(appName):
    cur = getConnection().cursor()
    cur.execute('''
SELECT name, uid, name || '-' || uid as version, date, git_hash, username, options
FROM app_versions LEFT OUTER JOIN compiles USING (name, uid) WHERE name='%s';
''' % appName)
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
  cur.execute("INSERT INTO jobs VALUES ('%(jobId)d', '%(appName)s', '%(appUID)s', '%(user)s', '%(time)s', '%(status)s');" %
              dict(job, jobId=jobId, status='SUBMITTED', time=time))
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
  return [dict(jobId=r[0], appName=r[1], hash=r[2], user=r[3], time=r[4], status=r[5]) for r in cur.fetchall()]

def updateJob(jobId, **kwargs):
  status = kwargs.get("status", None)
  conn = getConnection()
  cur = conn.cursor()

  if status:
    cur.execute("UPDATE jobs SET status='%s' WHERE jobId=%s;" % (status, jobId))
    conn.commit()

def insertCompile(comp):
  conn = getConnection()
  cur = conn.cursor()
  time = str(getTS_utc())
  cur.execute('''
INSERT INTO compiles (name, uid, git_hash, username, options, submit, status)
VALUES('%(name)s', '%(uid)s', '%(git_hash)s', '%(user)s', '%(options)s', '%(time)s', '%(status)s');
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
  active  = kwargs.get('active', False)

  # filter = ("" if not appName and not jobId
  #           else (" WHERE appName='%s'" % appName if appName
  #                 else " WHERE jobId='%s'" % jobId))

  cur = getConnection().cursor()
  cur.execute("SELECT * from compiles ORDER BY submit DESC;")
  return dict(name=r[0], uid=r[1], git_hash=r[2], user=r[3], options=r[4], submit=r[5], complete=r[6], status=r[7])
