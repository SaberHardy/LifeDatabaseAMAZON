import mysql.connector
import psycopg2

# establishing the connection
conn = psycopg2.connect(
    database="testdb",
    user='consultants',
    password='WelcomeItc@2022',
    host='ec2-3-9-191-104.eu-west-2.compute.amazonaws.com',
    port='5432'
)

# conn.autocommit = True

cursor = conn.cursor()

sql = '''SELECT * FROM FraudDetection'''
cursor.execute(sql)
print("Table SELECTED successfully........")
res = cursor.fetchall()
print(res)
conn.commit()
# Close the cursor and connection
cursor.close()
conn.close()
