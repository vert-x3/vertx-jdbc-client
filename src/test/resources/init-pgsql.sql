CREATE TABLE temporal_data_type
(
  "id"          INTEGER NOT NULL PRIMARY KEY,
  "Date"        date,
  "Time"        time without time zone,
  "TimeTz"      time with time zone,
  "Timestamp"   timestamp without time zone,
  "TimestampTz" timestamp with time zone,
  "Interval"    interval
);

--- TemporalDataType
INSERT INTO temporal_data_type ("id", "Date", "Time", "TimeTz", "Timestamp", "TimestampTz", "Interval")
VALUES (1, '1981-05-30', '17:55:04.90512', '17:55:04.90512+03:07', '2017-05-14 19:35:58.237666',
        '2017-05-14 23:59:59.237666-03', '10 years 3 months 332 days 20 hours 20 minutes 20.999999 seconds');
