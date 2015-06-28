drop table if exists Globals cascade;
drop table if exists Messages cascade;

create table Globals (
  job_id             int,
  mess_id            int,
  dest_peer          text,
  key                text,
  value              json
);

create table Messages (
  job_id             int,
  mess_id            int,
  dest_peer          text,
  trigger            text,
  source_peer        text,
  contents           json,
  time               int
);

drop table if exists peers;
create table peers(job_id int, address text, name text);

drop table if exists keys;
create table keys(job_id int, key text);

----------------------------------------------
--
-- Functions for analysing ktrace in postgres

-- select all messages
drop function if exists msgs_ordered(int);
create function msgs_ordered(job_id int)
returns table(id integer, source text, dest text, trigger text, contents json) as $$
begin
  return query
  select mess_id as id, p1.name as src, p2.name as dst, m.trigger, m.contents
  from messages as m, peers as p1, peers as p2
  where m.job_id      = job_id
  and   m.source_peer = p1.address
  and   m.dest_peer   = p2.address
  order by m.time, m.mess_id;
end;
$$ language plpgsql;

-- show state
drop function if exists show_state(int);
create function show_state(job_id int)
returns table(id integer, dest text, key text, value json) as $$
begin
  return query
  select g.mess_id as id, p.name as dest, g.key as key, g.value as value
  from globals as g, peers as p, keys as k
  where g.job_id    = job_id
  and   g.key       = k.key
  and   g.dest_peer = p.address
  order by mess_id;
end;
$$ language plpgsql;

-- show changed state
drop function if exists show_changes_raw(int);
create function show_changes_raw(job_id int)
returns table(id integer, dest text, key text, value json) as $$
begin
  return query
  select g2.mess_id as id, g2.dest_peer, g2.key as key, g2.value as value
  from globals as g1, globals as g2
  where g1.job_id      =  job_id
  and   g1.job_id      =  g2.job_id
  and   g2.mess_id     =  g1.mess_id + 1
  and   g1.dest_peer   =  g2.dest_peer
  and   g1.key         =  g2.key
	and   g1.value::text <> g2.value::text
  order by id;
end;
$$ language plpgsql;

-- show changed state
drop function if exists show_changes(int);
create function show_changes(job_id int)
returns table(id integer, dest text, key text, value json) as $$
begin
  return query
  select c.id, p.name as dest, c.key, c.value
  from show_changes_raw(job_id) as c, peers as p
  where c.dest_peer = p.address
  order by id;
end;
$$ language plpgsql;

-- show changed state with messages
drop function if exists msgs_d(int);
create function msgs_d(job_id int)
returns table(id integer, src text, dst text, trigger text, args text, key text, value json) as $$
begin
  create temp table tmp on commit drop as
  select m.time, m.mess_id as id, m.trigger, m.source_peer, m.dest_peer, m.contents as args, c.key, c.value
  from
    ( select * from messages where m.job_id = job_id) as m
    left outer join
    show_changes_raw(job_id) as c
  on c.id = m.mess_id and
     c.dest = m.dest_peer;

  return query
  select tmp.id, p1.name, p2.name, tmp.trigger, substring(tmp.args::text, 1, 30), tmp.key, tmp.value
  from peers as p1, peers as p2, tmp
  where p1.address = tmp.source_peer and p2.address = tmp.dest_peer
  order by tmp.time, tmp.id;
end;
$$ language plpgsql;

-- Get final frontier of json values
create or replace function json_last(input json)
returns json as $$
  import json

  val = json.loads(input)
  # check that we match our desired input
  if not (type(val) is list and len(val) > 0 and type(val[0]) is list and
     len(val[0]) >= 2 and type(val[0][0]) is list and len(val[0][0]) == 2 and
     type(val[0][0][0]) is int and type(val[0][0][1]) is int):
    return input
  res = {}
  for x in val:
    if type(x) is list and len(x) >= 2:
      # split into vid, key, value
      vid = []
      key = []
      val = []
      l = len(x)
      for (i, k) in enumerate(x):
        if i == 0:
          vid = k
        elif i >= l - 1:
          val = k
        else:
          key += [k]
      # convert list to tuple (immutable)
      key = tuple(key)
      if key in res:
        # set result to max vid value
        (oldvid, oldv) = res[key]
        # compare vids
        if vid[0] > oldvid[0] or vid[0] == oldvid[0] and vid[1] > oldvid[1]:
          res[key] = (vid, val)
      else:
        res[key] = (vid, val)
  # convert res to list
  res2 = []
  for k in res:
    v = list(k) + [res[k][1]]
    res2.append(v)
  return json.dumps(res2)
$$ language plpythonu;

-- final state
drop function if exists final_state(int);
create or replace function final_state(job_id int)
returns table(id integer, peer text, key text, value json) as $$
begin
  return query
  select g.mess_id, p.name, g.key, json_last(g.value)
  from
    (select max(mess_id) as last_id, g.dest_peer as peer
     from globals as g
     where g.job_id = job_id
     group by g.dest_peer)
    as max_ids, globals as g, peers as p
  where g.job_id    = job_id
  and   g.mess_id   = max_ids.last_id
  and   g.dest_peer = max_ids.peer
  and   g.dest_peer = p.address
  order by g.mess_id;
end;
$$ language plpgsql;

