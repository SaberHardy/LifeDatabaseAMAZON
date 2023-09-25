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

sql = '''CREATE TABLE IF NOT EXISTS FraudDetection(
step VARCHAR(50),
type VARCHAR(50),
amount VARCHAR(50),
nameOrig VARCHAR(50),
oldbalanceOrg VARCHAR(50),
newbalanceOrig VARCHAR(50),
nameDest VARCHAR(50),
oldbalanceDest VARCHAR(50),
newbalanceDest VARCHAR(50),
isFraud VARCHAR(50),
isFlaggedFraud VARCHAR(50)
)'''
cursor.execute(sql)
print("Table created successfully........")
conn.commit()
# Close the cursor and connection
cursor.close()
conn.close()
