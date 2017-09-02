drop   table if     exists trip;
create table if not exists trip
(
    _c0    varchar(40) not null, # medallion
    _c1    varchar(40) not null, # hack_license
    _c2    varchar(10),          # vendor_id
    _c3    varchar(10),          # rate_code
    _c4    varchar(8),           # store_and_fwd_flag
    _c5    datetime,             # pickup_datetime
    _c6    datetime,             # dropoff_datetime
    _c7    tinyint,              # passenger_count
    _c8    integer,              # trip_time_in_secs
    _c9    float,                # trip_distance
    _c10   double not null,      # pickup_longitude
    _c11   double not null,      # pickup_latitude
    _c12   double not null,      # dropoff_longitude
    _c13   double not null,      # dropoff_latitude

    index pickup (_c10, _c11),
    index dropoff(_c12, _c13),
    index t_pickup(_c5),
    index t_dropoff(_c6),
    index license  (_c0, _c1)
)
row_format = compressed 
partition by linear key(_c0, _c1) partitions 256;
