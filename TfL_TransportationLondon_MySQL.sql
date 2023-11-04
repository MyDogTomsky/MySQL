## The dataset was brought from Kaggle, which was organised by HRISTO MAVRODIEV. 
## And Transport for London(TfL) grants the license to public.
## Thank you for organising the dataset for multiple years to HRISTO and,
## being open to the public to TfL
## Kaggle DATASET Link: [https://www.kaggle.com/datasets/hmavrodiev/london-bike-sharing-dataset]
## TfL DATASET Link: [https://cycling.data.tfl.gov.uk/]
##--------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bike_variation;

USE bike_variation;
SHOW TABLES;

CREATE TABLE IF NOT EXISTS london_merged(timestamp DATETIME, cnt INT,
t1 DOUBLE, t2 DOUBLE, wind_speed DOUBLE, hum DOUBLE, weather_code int,is_holiday int,
is_weekend int, season int);

LOAD DATA LOCAL INFILE 'D:\\SQL\\CFG\\submisson\\london_merged.csv'
INTO TABLE london_merged
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
DESC london_merged;

CREATE TABLE time_count(timestamp datetime, cnt int);
CREATE TABLE weather(timestamp datetime, t1 double, t2 double, hum double, weather_code int);
CREATE TABLE holiday(timestamp datetime, is_holiday int);
CREATE TABLE weekend(timestamp datetime, is_weekend int);
CREATE TABLE season(timestamp datetime, season int);

INSERT INTO time_count(timestamp,cnt) SELECT timestamp, cnt FROM london_merged;
INSERT INTO weather(timestamp,t1, t2, hum, weather_code) SELECT timestamp,t1, t2, hum, weather_code FROM london_merged; 
INSERT INTO holiday(timestamp, is_holiday) SELECT timestamp, is_holiday FROM london_merged;
INSERT INTO weekend(timestamp, is_weekend) SELECT timestamp, is_weekend FROM london_merged;
INSERT INTO season(timestamp, season) SELECT timestamp, season FROM london_merged;

SHOW TABLES;
SELECT* FROM holiday;
SELECT* FROM weekend;
SELECT* FROM season;
SELECT* FROM weather;
SELECT* FROM london_merged;
SELECT* FROM time_count;

ALTER TABLE time_count ADD CONSTRAINT pk_timestamp PRIMARY KEY(timestamp);
ALTER TABLE holiday ADD CONSTRAINT fk_timestamp FOREIGN KEY(timestamp) REFERENCES time_count(timestamp);
ALTER TABLE season ADD CONSTRAINT fk_timestampS FOREIGN KEY(timestamp) REFERENCES time_count(timestamp);
ALTER TABLE weekend ADD CONSTRAINT fk_timestampW FOREIGN KEY(timestamp) REFERENCES time_count(timestamp);
ALTER table weather add constraint fk_timeweather FOREIGN KEY(timestamp) REFERENCES time_count(timestamp);

#-- CREATE VIEW with multiple JOINs --- 

CREATE VIEW listAll_DESC AS 
SELECT tc.timestamp, tc.cnt, wt.t1, se.season, hi.is_holiday, wk.is_weekend
FROM time_count AS tc
INNER JOIN weather AS wt ON tc.timestamp = wt.timestamp
INNER JOIN weekend AS wk ON tc.timestamp = wk.timestamp
INNER JOIN season AS se ON tc.timestamp = se.timestamp
INNER JOIN holiday AS hi ON tc.timestamp = hi.timestamp  
ORDER BY tc.cnt DESC;

SELECT * FROM listAll_DESC;
#--------------------------------------

#-- Checking missing data in the 'weekend' table -------

SELECT wk.timestamp, wk.is_weekend, tc.cnt
FROM weekend AS wk 
LEFT JOIN time_count tc ON wk.timestamp = tc.timestamp
ORDER BY wk.is_weekend DESC;
#-------------------------------------------------------

#-- A new intended query by using subquery: season table------

SELECT* FROM season;
SELECT se.season, seq.seasonTimes, sum(tc.cnt) AS totalRide 
FROM season AS se
INNER JOIN (SELECT season, COUNT(season) AS seasonTimes
			FROM season
			GROUP BY season) AS seq ON se.season = seq.season
INNER JOIN time_count AS tc ON tc.timestamp = se.timestamp
GROUP BY se.season, seq.seasonTimes
HAVING se.season IN (0,1,2)
ORDER BY se.season ASC;

SELECT* FROM season;
#-------------------------------------------------------------------

#---- Making a stored Function: numerical data --> string data --------

DELIMITER //
CREATE FUNCTION FEEL(t1 DOUBLE) RETURNS VARCHAR(10)
DETERMINISTIC
BEGIN 
	DECLARE FEEL VARCHAR(10);
	IF t1 >= 23 THEN SET FEEL = 'WARM';
	ELSE SET FEEL = 'COLD'; 
	END IF;
    RETURN FEEL;
END//
DELIMITER ;

# ---------------- CHECKING THE FUNCTION to the NEW VIEW ---------------

CREATE VIEW listAll_WARMTH AS 
SELECT tc.timestamp, tc.cnt, FEEL(wt.t1) AS temp_Feel, se.season, hi.is_holiday, wk.is_weekend
FROM time_count AS tc
INNER JOIN weather AS wt ON tc.timestamp = wt.timestamp
INNER JOIN weekend AS wk ON tc.timestamp = wk.timestamp
INNER JOIN season AS se ON tc.timestamp = se.timestamp
INNER JOIN holiday AS hi ON tc.timestamp = hi.timestamp  
ORDER BY tc.cnt DESC;
 
SELECT* FROM listAll_DESC; 
SELECT* FROM listAll_WARMTH;
#------------------------------------------------------------------------

#--- Making event (1) DROP A FOREIGN KEY AND SET DROP AUTOMATICALLY (2) MAKING AN EVENT

ALTER TABLE weather ADD CONSTRAINT FOREIGN KEY(timestamp) REFERENCES time_count(timestamp) ON DELETE CASCADE;
ALTER TABLE holiday ADD CONSTRAINT FOREIGN KEY(timestamp) REFERENCES time_count(timestamp) ON DELETE CASCADE;
ALTER TABLE weekend ADD CONSTRAINT FOREIGN KEY(timestamp) REFERENCES time_count(timestamp) ON DELETE CASCADE;
ALTER TABLE season  ADD CONSTRAINT FOREIGN KEY(timestamp) REFERENCES time_count(timestamp) ON DELETE CASCADE;

#---------EVENT ------------------------

SET GLOBAL event_scheduler = ON;
SHOW VARIABLES LIKE 'event_scheduler';

CREATE EVENT remove_old ON SCHEDULE EVERY 1 YEAR
STARTS CURRENT_TIMESTAMP
DO
DELETE FROM time_count WHERE timestamp < DATE_SUB(NOW(), INTERVAL 7 YEAR);
SHOW CREATE EVENT remove_old;

SELECT* FROM weather ORDER BY timestamp ASC;