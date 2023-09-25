import psycopg2
import csv

# Connect to PostgreSQL (replace with your connection details)
conn = psycopg2.connect(
    database="testdb",
    user='consultants',
    password='WelcomeItc@2022',
    host='ec2-3-9-191-104.eu-west-2.compute.amazonaws.com',
    port='5432'
)

# Create a cursor object
cur = conn.cursor()

# Execute the SQL command to create the table
# cur.execute(create_table_query)

# Read data from CSV file and insert into the table
with open('EndToEndProject.csv', 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud = row  # Adjust based on your CSV structure
        cur.execute(
            "INSERT INTO FraudDetection (step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,"
            "nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud) "
            "VALUES "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
            (step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest,
             isFraud, isFlaggedFraud))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
