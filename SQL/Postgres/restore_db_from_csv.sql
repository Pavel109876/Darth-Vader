create table drivers (driverId int, driverRef text, number text, code text, 
forename text, surname text, dob date, nationality text, url text)

copy public.drivers from 'D:\test\drivers.csv' delimiter ',' csv header;

select * from public.drivers where UPPER(forename) = 'MAX'

UPDATE public.drivers SET number = NULL WHERE number = '\N';
UPDATE public.drivers SET code = NULL WHERE code = '\N';

ALTER TABLE public.drivers
ALTER COLUMN number TYPE INT 
USING number::integer;

--test2222