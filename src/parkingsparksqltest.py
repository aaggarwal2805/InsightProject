from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
import collections
from pyspark.sql import functions
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import unix_timestamp, col
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, date_trunc

sc = SparkContext(appName="PythonStreamingQueueStream")

# Create a SparkSession
spark = SparkSession.builder.appName("ParkingSQL").enableHiveSupport().getOrCreate()

year2013 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("/home/anshu/InsightFellowship/ParkingLookUp/data/On-street_Car_Parking_Sensor_Data_-_2013_test.csv")
year2013.createOrReplaceTempView("year2013")

dt = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("/home/anshu/InsightFellowship/ParkingLookUp/data/DT.csv")
dt.createOrReplaceTempView("dt")

#spark.sql("CREATE TABLE INPUTDAYTIME AS (SELECT CAST(dt.DOW AS INT) AS DOW, dt.Day, CAST(dt.Time AS INT) AS Time FROM dt)")

#spark.sql("CREATE TABLE 2013_dev AS (SELECT year2013.DeviceId, year2013.ArrivalTime, year2013.DepartureTime  FROM year2013)")


#spark.sql("CREATE TABLE FORMAT(SELECT CAST(2013_dev.DeviceId AS INT) AS DeviceId, TO_TIMESTAMP(CAST (UNIX_TIMESTAMP(2013_dev.ArrivalTime, 'MM/dd/yyyy h:mm:ss a') AS TIMESTAMP)) AS AT, TO_TIMESTAMP(CAST (UNIX_TIMESTAMP(2013_dev.DepartureTime, 'MM/dd/yyyy h:mm:ss a') AS TIMESTAMP)) AS DT FROM 2013_dev)")
#spark.sql("SELECT * FROM FORMAT")
#df.printSchema()

#spark.sql("CREATE TABLE DeviceDayTime AS (select FORMAT.DeviceId, dayOfWeek(FORMAT.AT) AS DAY, hour(FORMAT.AT) AS TIME FROM FORMAT)")

#spark.sql("CREATE TABLE DVD AS (SELECT DeviceDayTime.DeviceId, INPUTDAYTIME.Day, DeviceDayTime.TIME FROM DeviceDayTime JOIN INPUTDAYTIME ON DeviceDayTime.DAY==INPUTDAYTIME.DOW)")

spark.sql("SELECT COUNT(DISTINCT DVD.DeviceId), DVD.DeviceID, DVD.Day, DVD.Time FROM DVD WHERE DVD.Day = 'Mon' AND DVD.Time = 9 GROUP BY DVD.DeviceId, DVD.Day, DVD.Time")
#DVD WHERE (DVD.Day = 'Mon' AND DVD.TIME = 9)").show()

'''
bay = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("/home/anshu/InsightFellowship/ParkingLookUp/data/On-street_Parking_Bay_Sensors.csv")
bay.createOrReplaceTempView("bay")
#spark.sql("CREATE TABLE BAYLAT AS (SELECT bay.lat FROM bay)")
#output = spark.sql("SELECT * FROM BAYLAT")
spark.sql("SELECT CAST(BAYLAT.lat AS DECIMAL(30,20)) AS lat FROM BAYLAT").show()
#output.show()
'''
