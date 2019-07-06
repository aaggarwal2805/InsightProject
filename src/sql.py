DB_HOST ="rds-instance.cjxcefmwyzy2.us-east-1.rds.amazonaws.com"
DB_PORT = 5432
DB_USER ="dbuser"
DB_PASSWD ="****"
DB_NAME ="parkingdb"
import psycopg
mydb = psycopg.connect(host = DB_HOST,port = DB_PORT,user = DB_USER,passwd= DB_PASSWD,db= DB_NAME)
mycursor = mydb.cursor()
sql="show schemas"
mycursor.execute(sql)
 
