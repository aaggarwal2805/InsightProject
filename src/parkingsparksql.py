from pyspark.sql import SparkSession
from pyspark.sql import Row
import collections


# Create a SparkSession
spark = SparkSession.builder.appName("ParkingSQL").enableHiveSupport().getOrCreate()

#Create DataFrames
restriction = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://sensorlocationdata/On-street_Car_Park_Bay_Restrictions.csv")
baydevice = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://sensorlocationdata/On-street_Parking_Bay_Sensors.csv")
year2017 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://sensordevicedata/On-street_Car_Parking_Sensor_Data_-_2017.csv")
year2016 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://sensordevicedata/On-street_Car_Parking_Sensor_Data_-_2016.csv")
year2015 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://sensordevicedata/On-street_Car_Parking_Sensor_Data_-_2015.csv")
year2014 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://sensordevicedata/On-street_Car_Parking_Sensor_Data_-_2014.csv")
year2013 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://sensordevicedata/On-street_Car_Parking_Sensor_Data_-_2013.csv")
year2012 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://sensordevicedata/On-street_Car_Parking_Sensor_Data_-_2012.csv")
year2011 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://sensordevicedata/On-street_Car_Parking_Sensor_Data_-_2011.csv")
dt = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("s3n://daytimeinput/DT.csv")


restriction.createOrReplaceTempView("restriction")
baydevice.createOrReplaceTempView("baydevice")
year2017.createOrReplaceTempView("year2017")
year2016.createOrReplaceTempView("year2016")
year2015.createOrReplaceTempView("year2015")
year2014.createOrReplaceTempView("year2014")
year2013.createOrReplaceTempView("year2013")
year2012.createOrReplaceTempView("year2012")
year2011.createOrReplaceTempView("year2011")
dt.createOrReplaceTempView("dt")

#UNION OF TABLES (2011-2017)

spark.sql("CREATE TABLE 1117alldata AS (SELECT year2017.DeviceId, year2017.ArrivalTime, year2017.DepartureTime FROM year2017 UNION ALL SELECT year2016.DeviceId, year2016.ArrivalTime, year2016.DepartureTime FROM year2016 UNION ALL SELECT year2015.DeviceId, year2015.ArrivalTime, year2015.DepartureTime FROM year2015 UNION ALL SELECT year2014.DeviceId, year2014.ArrivalTime, year2014.DepartureTime FROM year2014 UNION ALL SELECT year2013.DeviceId, year2013.ArrivalTime, year2013.DepartureTime FROM year2013 UNION ALL SELECT year2012.DeviceId, year2012.ArrivalTime, year2012.DepartureTime FROM year2012 UNION ALL SELECT year2011.DeviceId, year2011.ArrivalTime, year2011.DepartureTime FROM year2011)")
spark.sql("CREATE TABLE bt AS (SELECT * FROM baydevice)")
spark.sql("CREATE TABLE rt AS (SELECT * FROM restriction)")
spark.sql("CREATE TABLE c2017 AS (SELECT CAST(all2017.DeviceId AS INT) AS DeviceId, TO_TIMESTAMP(CAST (UNIX_TIMESTAMP(all2017.ArrivalTime, 'MM/dd/yyyy h:mm:ss a') AS TIMESTAMP)) AS ArrivalTime, TO_TIMESTAMP(CAST (UNIX_TIMESTAMP(all2017.DepartureTime, 'MM/dd/yyyy h:mm:ss a') AS TIMESTAMP)) AS DepartureTime, CAST(all2017.DurationSeconds AS INT) AS OccupiedDuration FROM all2017)")
spark.sql("CREATE TABLE 2017daytime AS (SELECT c2017.DeviceId, dayOfWeek(c2017.ArrivalTime) AS Day, hour(c2017.ArrivalTime) AS Time, c2017.OccupiedDuration FROM c2017)")
spark.sql("CREATE TABLE aratio AS (SELECT 2017daytime.DeviceId, 2017daytime.Day, 2017daytime.Time, SUM(2017daytime.OccupiedDuration) AS Ocupied, 165392*3600 AS total FROM 2017daytime GROUP BY 2017daytime.DeviceId, 2017daytime.Day, 2017daytime.Time)")
spark.sql("CREATE TABLE b AS (SELECT aratio.DeviceId, aratio.Day, aratio.Time, aratio.Ocupied, (aratio.total-aratio.Ocupied) AS Free, aratio.total  FROM aratio)")
spark.sql("CREATE TABLE parkper AS (SELECT b.DeviceId, b.day, b.time, (b.free/b.total)*100 AS per FROM b)")
spark.sql("CREATE TABLE DT AS (SELECT CAST(dt.DOW AS INT) AS DOW, dt.Day, CAST(dt.Time AS INT) AS Time FROM dt)")
spark.sql("CREATE TABLE BayL AS (SELECT rt.DEVICE_ID, bt.location, bt.lat, bt.lon FROM rt LEFT JOIN bt ON bt.bay_id == rt.BAY_ID)")
spark.sql("CREATE TABLE a AS (SELECT parkper.DeviceId, dt.day, parkper.time, parkper.per FROM parkper INNER JOIN DT ON parkper.day=dt.dow)")
spark.sql("CREATE TABLE c AS (SELECT a.DeviceId, a.day, a.time, a.per, CAST(BayL.lat AS DECIMAL(30,20)) AS lat, CAST(BayL.lon AS DECIMAL(30,20)) AS lan FROM a LEFT JOIN BayL ON a.DeviceId=BayL.Device_ID WHERE bayl.lat IS NOT NULL)").show()
df = spark.sql("SELECT * FROM c")

#Writing to postgreSQL table
mode = "overwrite"
url = "jdbc:postgresql://198.123.43.24:5432/parkingapp"
properties = {"user": "postgres","password": "password","driver": "org.postgresql.Driver"}
df.write.jdbc(url=url, table="pt", mode=mode, properties=properties)
