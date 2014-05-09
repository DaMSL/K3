-- DB schema
create table log_data
(
  log text,
  t integer,
  address text,
  trigger text,
  map text,
  data text
);

-- Copy the new data into the database
copy log_data from $file_path delimiter '/';

-- A simple view of the protocol
select * from log_data where map = 'args';

-- Get the maximum t per log we have in the db
select log, max(t) as max_t from log_data group by log;

