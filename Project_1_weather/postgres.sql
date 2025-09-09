CREATE TYPE weather_temp as (
		time timestamp,
		description varchar,
		temp numeric,
		wind_speed numeric
)

-- drop TYPE weather_temp
-- drop table weather

-- CREATE TABLE IF NOT EXISTS weather_default PARTITION OF weather DEFAULT;

--  SELECT column_name, data_type, is_nullable
-- FROM information_schema.columns
-- WHERE table_name = 'weather';

CREATE  TABLE weather (
	id bigint GENERATED ALWAYS AS IDENTITY,
	date date,
	country varchar,
	city varchar,
	temp weather_temp[],
	primary KEY(id,date)
) partition by RANGE (date);

