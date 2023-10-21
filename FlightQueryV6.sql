
DROP TABLE IF EXISTS Aircraft;
CREATE TABLE Aircraft (
  aircraft_id integer auto_increment,
  aircraft_name varchar(100),
  IATA varchar(10),
  ICAO varchar(10),
  PRIMARY KEY(aircraft_id)
);

DROP TABLE IF EXISTS Passanger;
CREATE TABLE Passanger (
  passanger_id integer PRIMARY KEY,
  first_name varchar(50),
  last_name varchar(50),
  age integer,
  gender ENUM('M','F')
);

DROP TABLE IF EXISTS Flight_Class;
CREATE TABLE Flight_Class (
  class_id integer PRIMARY KEY,
  description varchar(20)
);

DROP TABLE IF EXISTS Airline;
CREATE TABLE Airline (
  airline_id integer PRIMARY KEY,
  airline_name varchar(50),
  IATA varchar(10),
  ICAO varchar(10),
  `status` ENUM('Y','N'),
  callsign varchar(50)
);

DROP TABLE IF EXISTS Airport;
CREATE TABLE Airport(
  airport_id integer auto_increment,
  airport_name varchar(50),
  IATA varchar(10),
  ICAO varchar(10),
  longitude_direction char(1),
  latitude_direction char(1),
  longitude_decimal_degrees double,
  latitude_decimal_degrees double,
  altitude double,
  city_id integer ,
  city_name varchar(100),
  country_id integer,
  country_name varchar(50),
  iso_code varchar(5),
  dafif_code varchar(5),
  PRIMARY KEY(airport_id)
);

DROP TABLE IF EXISTS `Time`;
CREATE TABLE `Time`(
  time_id integer auto_increment,
  `hour` integer(2) CHECK (`hour` >= 0 AND `hour` <= 23), 
  `minute` integer(2) CHECK (`minute` >= 0 AND `minute` <= 59),
  `second` integer(2) CHECK (`second` >= 0 AND `second` <= 59),
  PRIMARY KEY(time_id)
);

DROP TABLE IF EXISTS `Date`;
CREATE TABLE `Date`(
  date_id integer auto_increment,
  `year` integer(4),
  `month` integer(2) CHECK (`month` >= 1 AND `month` <= 12),
  `day` integer(2) CHECK (`day` >= 1 AND `day` <= 31),
  PRIMARY KEY(date_id)
);

DROP TABLE IF EXISTS `Schedule`;
CREATE TABLE `Schedule`(
  schedule_id integer auto_increment,
  departure_date date,
  departure_time time,
  actual_departure_date date,
  actual_departure_time  time,
  arrival_date  date,
  arrival_time time,
  actual_arrival_date date,
  actual_arrival_time time,
  PRIMARY KEY(schedule_id)
);


DROP TABLE IF EXISTS StatisticsPeriod;
CREATE TABLE StatisticsPeriod(
statisticsperiod_id integer auto_increment,
`year` integer(4),
`month` integer(2) CHECK (`month` >= 1 AND `month` <= 12),
PRIMARY KEY(statisticsperiod_id)
);

DROP TABLE IF EXISTS Booking;
CREATE TABLE Booking(
passanger_id integer references Passanger(passanger_id),
class_id integer references Flight_Class(class_id),
origin_airport_id integer references Airport(airport_id),
arrival_airport_id integer references Airport(airport_id),
time_id integer references `Time`(time_id),
date_id integer references `Date`(date_id),
seat varchar(10),
nr_stops integer(10),
duration_stops double
);

DROP TABLE IF EXISTS Flight;
CREATE TABLE Flight(
flight_id varchar(50) primary key,
airline_id integer references Airline(airline_id),
aircraft_id integer references Aircraft(aircraft_id),
origin_airport_id integer references Airport(airport_id),
arrival_airport_id integer references Airport(airport_id),
schedule_id integer references `Schedule`(schedule_id),
miles double, 
nr_passenger int
);

DROP TABLE IF EXISTS Flight_Statistics;
CREATE TABLE Flight_Statistics(
airline_id integer references Airline(airline_id),
airport_id integer references Airport(airport_id),
statisticsperiod_id integer references StatisticsPeriod(statisticsperiod_id),
nr_arrival_delays integer,
nr_departure_delays integer,
nr_on_time_departure integer,
nr_on_time_arrivals integer,
nr_delays integer AS (nr_arrival_delays + nr_departure_delays)
);

-- load Passanger
insert into Passanger(passanger_id, first_name, last_name, age, gender)
select CustId,Cust_F_Name,Cust_L_Name,Cust_Age,Cust_Gender
from raw_data_passangers_datasets;


-- load Flight_Class
insert into Flight_Class(class_id, description) values (1,'Economic Class');
insert into Flight_Class(class_id, description) values (2,'Business Class');

-- load Aircraft
insert into Aircraft(aircraft_name, IATA, ICAO)
select `Name`,`IATA Code`,`ICAO code`
from raw_data_airplanes;


-- load Airline
insert into Airline(airline_id, airline_name, IATA, ICAO, `status`, callsign)
select `Airline ID`,`Name`,IATA,ICAO,Active,Callsign
from raw_data_airlines;

-- load Airport
SET @row_number = 0;
INSERT INTO Airport (airport_name, IATA, ICAO, longitude_direction, latitude_direction, 
  longitude_decimal_degrees, latitude_decimal_degrees, altitude, city_id, city_name, 
  country_id, country_name, iso_code, dafif_code)
SELECT `Airport Name`, `IATA Code`, `ICAO Code`, `Longitude Direction`, `Latitude Direction`,
  `Longitude Decimal Degrees`, `Latitude Decimal Degrees`, Altitude, T.city_id, T.city, 
  c.id, c.name, c.iso_code, c.dafif_code 
FROM raw_data_globalairport r 
LEFT JOIN raw_data_countries c ON lower(c.name) = lower(r.Country)
JOIN (
  SELECT `City/Town` as city, (@row_number:=@row_number+1) AS city_id
  FROM raw_data_globalairport
  GROUP BY `City/Town`
) AS T ON T.city = r.`City/Town`;


-- load Schedule
insert into `Schedule`(departure_date, departure_time, actual_departure_date, actual_departure_time,
						arrival_date, arrival_time, actual_arrival_date, actual_arrival_time)
SELECT STR_TO_DATE(Scheduled_Departure_Date, '%m/%d/%Y') AS departure_date,
	TIME(STR_TO_DATE(CONCAT(Scheduled_Departure_Date, ' ', Scheduled_Departure_Time),'%m/%d/%Y %H:%i:%s')) AS departure_time,
	STR_TO_DATE(Actual_Departure_Date, '%m/%d/%Y') AS actual_departure_date,
	TIME(STR_TO_DATE(CONCAT(Actual_Departure_Date, ' ',Actual_Departure_Time),'%m/%d/%Y %H:%i:%s')) AS actual_departure_time,
	STR_TO_DATE(Scheduled_Arrival_Date, '%m/%d/%Y') AS arrival_date,
	TIME(STR_TO_DATE(CONCAT(Scheduled_Arrival_Date,' ',Scheduled_Arrival_Time),'%m/%d/%Y %H:%i:%s')) AS arrival_time,
	STR_TO_DATE(Actual_Arrival_Date, '%m/%d/%Y') AS actual_arrival_date,
	TIME(STR_TO_DATE(CONCAT(Actual_Arrival_Date,' ', Actual_Arrival_Time),'%m/%d/%Y %H:%i:%s')) AS actual_arrival_time
	from raw_data_passangers_datasets;


-- load Date
insert into `Date`(`year`, `month` , `day`)
select YEAR(STR_TO_DATE(Date_Of_Booking, '%d-%b-%Y') ) as year,
MONTH(STR_TO_DATE(Date_Of_Booking, '%d-%b-%Y')) AS month,
DAY(STR_TO_DATE(Date_Of_Booking, '%d-%b-%Y')) AS day
from  raw_data_passangers
GROUP BY Date_Of_Booking;

-- load Time
insert into `Time`(`hour`, `minute` , `second`)
select 
  HOUR(TIME(Time_Of_Booking)) as hour, 
  MINUTE(TIME(Time_Of_Booking)) as minute, 
  SECOND(TIME(Time_Of_Booking)) as second
from raw_data_passangers_datasets
GROUP BY Time_Of_Booking ;


-- load Statisticsperiod CONCAT(`year`, "-", `month`) AS `year_month`
INSERT INTO Statisticsperiod (`year`, `month`)
SELECT YEAR(STR_TO_DATE(Scheduled_Departure_Date, '%m/%d/%Y') ) as year,
MONTH(STR_TO_DATE(Scheduled_Departure_Date, '%m/%d/%Y')) AS month
FROM raw_data_passangers_datasets
GROUP BY Scheduled_Departure_Date;


-- load Flight_Statistics

insert into Flight_Statistics(airline_id,airport_id,statisticsperiod_id,
			nr_departure_delays,nr_arrival_delays,nr_on_time_departure,nr_on_time_arrivals) 
select l.airline_id, a.airport_id,statisticsperiod_id, 
	SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, 
		STR_TO_DATE(CONCAT(p.Scheduled_Departure_Date, ' ', p.Scheduled_Departure_Time),'%m/%d/%Y %H:%i:%s'),
		STR_TO_DATE(CONCAT(p.Actual_Departure_Date, ' ', p.Actual_Departure_Time),'%m/%d/%Y %H:%i:%s')) > 0 
		THEN 1 ELSE 0 END) AS nr_departure_delays,
    SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, 
        STR_TO_DATE(CONCAT(p.Scheduled_Arrival_Date, ' ', p.Scheduled_Arrival_Time),'%m/%d/%Y %H:%i:%s'),
        STR_TO_DATE(CONCAT(p.Actual_Arrival_Date, ' ', p.Actual_Arrival_Time),'%m/%d/%Y %H:%i:%s')) > 0 
        THEN 1 ELSE 0 END) AS nr_arrival_delays,
    SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, 
        STR_TO_DATE(CONCAT(p.Scheduled_Departure_Date, ' ', p.Scheduled_Departure_Time),'%m/%d/%Y %H:%i:%s'),
        STR_TO_DATE(CONCAT(p.Actual_Departure_Date, ' ', p.Actual_Departure_Time),'%m/%d/%Y %H:%i:%s')) <= 0 
        THEN 1 ELSE 0 END) AS nr_on_time_departure,
    SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, 
       STR_TO_DATE(CONCAT(p.Scheduled_Arrival_Date, ' ', p.Scheduled_Arrival_Time),'%m/%d/%Y %H:%i:%s'),
       STR_TO_DATE(CONCAT(p.Actual_Arrival_Date, ' ', p.Actual_Arrival_Time),'%m/%d/%Y %H:%i:%s')) <= 0 
       THEN 1 ELSE 0 END) AS nr_on_time_arrivals
from raw_data_passangers_datasets p left join airport a on lower(p.Flying_From) = a.city_name left join
	Airline l on l.IATA = LEFT(p.Flight_Number,2) Left Join statisticsperiod s on 
    YEAR(STR_TO_DATE(Scheduled_Departure_Date, '%m/%d/%Y')) = s.year and 
    MONTH(STR_TO_DATE(Scheduled_Departure_Date, '%m/%d/%Y')) = s.month
Group by l.airline_id, a.airport_id, a.city_name, statisticsperiod_id ;
    
-- load  booking
INSERT INTO booking (passanger_id, class_id, origin_airport_id, arrival_airport_id, time_id, date_id, seat, nr_stops, duration_stops)
SELECT 
    p.passanger_id, r.Flight_Class,a1.airport_id AS origin_airport_id, a2.airport_id AS arrival_airport_id, 
    t.time_id, d.date_id, r.Seat_Allocated AS seat, IF(Stop_Over_1 != "NA" AND Stop_Over_2 != "NA", 2, IF(Stop_Over_1 != "NA" OR Stop_Over_2 != "NA", 1, 0)) AS nr_stops, 
    r.Break_in_Hours1 + r.Break_in_Hours2 AS duration_stops
FROM 
    raw_data_passangers_datasets AS r LEFT JOIN passanger AS p ON r.CustId = p.passanger_id 
    LEFT JOIN airport AS a1 ON LOWER(r.Flying_From) = LOWER(a1.city_name) 
    LEFT JOIN airport AS a2 ON LOWER(r.Flying_To) = LOWER(a2.city_name)
    LEFT JOIN `Time` t ON t.time_id = r.Time_Of_Booking 
    LEFT JOIN `Date` d ON d.date_id = r.Date_Of_Booking;

 select * from raw_data_passangers_datasets;
 select * from Airline;
 select * from Airport;
 select * from statisticsperiod;
#select * from raw_data_passangers_datasets;
#select * from raw_data_globalairport;
#select * from airport;
#select * from  `Schedule`;

-- function to calculate miles from altitiude and longitude

select calculate_distance(a1.latitude_decimal_degrees, a1.longitude_decimal_degrees,a2.latitude_decimal_degrees, a2.longitude_decimal_degrees)
 from airport a1 left join airport a2 on a1.airport_id = a2.airport_id;

select *  from airport a1 left join airport a2 on a1.airport_id = a2.airport_id;
select Flight_Number,count(*) from raw_data_passangers_datasets group by Flight_Number;

-- load Flight

insert into Flight(flight_id ,airline_id,aircraft_id, origin_airport_id, 
			arrival_airport_id, schedule_id, miles, nr_passenger) 
select p.Flight_Number as flight_id ,r.airline_id ,f.aircraft_id, 
	a1.airport_id as origin_airport_id, a2.airport_id as destination_airport_id , s.schedule_id,
    calculate_distance(a1.latitude_decimal_degrees, a1.longitude_decimal_degrees,a2.latitude_decimal_degrees, a2.longitude_decimal_degrees) as miles,
    p.number_of_passengers
 from (
  SELECT distinct Flight_Number, Aircraft,Flying_From,Flying_To,Scheduled_Departure_Time,Scheduled_Departure_Date,
  Actual_Departure_Time,Actual_Departure_Date,Scheduled_Arrival_Date,Scheduled_Arrival_Time,Actual_Arrival_Date,Actual_Arrival_Time,
  count(CustId) as number_of_passengers
  FROM raw_data_passangers_datasets
  group by Flight_Number, Aircraft,Flying_From,Flying_To,Scheduled_Departure_Time,
  Scheduled_Departure_Date,  Actual_Departure_Time,Actual_Departure_Date,Scheduled_Arrival_Date,
  Scheduled_Arrival_Time,Actual_Arrival_Date,Actual_Arrival_Time
) as p left join airline r on r.IATA = LEFT(p.Flight_Number,2)
left join aircraft f on f.IATA= p.Aircraft
LEFT JOIN airport AS a1 ON LOWER(p.Flying_From) = LOWER(a1.city_name) 
LEFT JOIN airport AS a2 ON LOWER(p.Flying_To) = LOWER(a2.city_name)
left join `schedule` s on s.departure_date = STR_TO_DATE(p.Scheduled_Departure_Date, '%m/%d/%Y')
and s.departure_time = 	TIME(STR_TO_DATE(CONCAT(p.Scheduled_Departure_Date, ' ',  p.Scheduled_Departure_Time),'%m/%d/%Y %H:%i:%s'))
 and s.actual_departure_date = STR_TO_DATE(p.Actual_Departure_Date, '%m/%d/%Y')
and s.actual_departure_time = TIME(STR_TO_DATE(CONCAT(p.Actual_Departure_Date, ' ',  p.Actual_Departure_Time),'%m/%d/%Y %H:%i:%s'))
 and s.arrival_date = STR_TO_DATE(p.Scheduled_Arrival_Date, '%m/%d/%Y')
and s.arrival_time = TIME(STR_TO_DATE(CONCAT(p.Scheduled_Arrival_Date, ' ',   p.Scheduled_Arrival_Time),'%m/%d/%Y %H:%i:%s'))
 and s.actual_arrival_date = STR_TO_DATE( p.Actual_Arrival_Date, '%m/%d/%Y')
and s.actual_arrival_time = TIME(STR_TO_DATE(CONCAT(p.Actual_Arrival_Date, ' ',    p.Actual_Arrival_Time),'%m/%d/%Y %H:%i:%s'));
 

-- set session wait_timeout=999999;
-- show variables like 'wait_timeout';


select p.Flight_Number as flight_id ,r.airline_id ,f.aircraft_id, 
	a1.airport_id as origin_airport_id, a2.airport_id as destination_airport_id , s.schedule_id,
    calculate_distance(a1.latitude_decimal_degrees, a1.longitude_decimal_degrees,a2.latitude_decimal_degrees, a2.longitude_decimal_degrees) as miles,
    p.number_of_passengers
 from (
  SELECT distinct Flight_Number, Aircraft,Flying_From,Flying_To,Scheduled_Departure_Time,Scheduled_Departure_Date,
  Actual_Departure_Time,Actual_Departure_Date,Scheduled_Arrival_Date,Scheduled_Arrival_Time,Actual_Arrival_Date,Actual_Arrival_Time,
  count(CustId) as number_of_passengers
  FROM raw_data_passangers_datasets
  group by Flight_Number, Aircraft,Flying_From,Flying_To,Scheduled_Departure_Time,
  Scheduled_Departure_Date,  Actual_Departure_Time,Actual_Departure_Date,Scheduled_Arrival_Date,
  Scheduled_Arrival_Time,Actual_Arrival_Date,Actual_Arrival_Time
) as p left join airline r on r.IATA = LEFT(p.Flight_Number,2)
left join aircraft f on f.IATA= p.Aircraft
LEFT JOIN airport AS a1 ON LOWER(p.Flying_From) = LOWER(a1.city_name) 
LEFT JOIN airport AS a2 ON LOWER(p.Flying_To) = LOWER(a2.city_name)
left join `schedule` s on s.departure_date = STR_TO_DATE(p.Scheduled_Departure_Date, '%m/%d/%Y')
and s.departure_time = 	TIME(STR_TO_DATE(CONCAT(p.Scheduled_Departure_Date, ' ',  p.Scheduled_Departure_Time),'%m/%d/%Y %H:%i:%s'))
 and s.actual_departure_date = STR_TO_DATE(p.Actual_Departure_Date, '%m/%d/%Y')
and s.actual_departure_time = TIME(STR_TO_DATE(CONCAT(p.Actual_Departure_Date, ' ',  p.Actual_Departure_Time),'%m/%d/%Y %H:%i:%s'))
 and s.arrival_date = STR_TO_DATE(p.Scheduled_Arrival_Date, '%m/%d/%Y')
and s.arrival_time = TIME(STR_TO_DATE(CONCAT(p.Scheduled_Arrival_Date, ' ',   p.Scheduled_Arrival_Time),'%m/%d/%Y %H:%i:%s'))
 and s.actual_arrival_date = STR_TO_DATE( p.Actual_Arrival_Date, '%m/%d/%Y')
and s.actual_arrival_time = TIME(STR_TO_DATE(CONCAT(p.Actual_Arrival_Date, ' ',    p.Actual_Arrival_Time),'%m/%d/%Y %H:%i:%s'));

