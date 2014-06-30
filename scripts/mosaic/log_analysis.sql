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

-- Get the maximum t per log and ip we have in the db
select log, address, max(t) as t from log_data group by log, address;

-- Create a view with the above
create view max_ts as select log, address, max(t) as t from log_data group by log, address;

-- Join with log_data
select * from log_data natural join max_ts;

