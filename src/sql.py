DB_HOST ="sql-instance.cjxcefmwyzy2.us-east-1.rds.amazonaws.com"
DB_PORT = 3306
DB_USER ="dbuser"
DB_PASSWD ="****"
DB_NAME ="parkingdb"
import MySQLdb
mydb = MySQLdb.connect(host = DB_HOST,port = DB_PORT,user = DB_USER,passwd= DB_PASSWD,db= DB_NAME)
mycursor = mydb.cursor()
sql="show schemas"
mycursor.execute(sql)
 
