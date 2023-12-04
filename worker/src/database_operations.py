#Code that includes interactions with the database, such as creating tables, inserting data, and executing queries.
import os
import mysql.connector

class ConectionDataBase():
    cnx = mysql.connector.connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'], host=os.environ['MYSQL_HOST'], port=os.environ['MYSQL_PORT'])
    cursor = cnx.cursor()

    def init_databaselogs(self):
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS databaselogs")
        self.cnx.database = "databaselogs"
        self.cursor.execute('DROP TABLE IF EXISTS logsfailure')
        self.cursor.execute('''CREATE TABLE logsfailure (
                        datahour DATETIME,
                        typelog VARCHAR(10),
                        sensor INT
                    )''')
        
        self.cursor.execute('DROP TABLE IF EXISTS sensors')
        self.cursor.execute('''CREATE TABLE sensors (
                        equipment_id INT,
                        sensor_id INT
                    )''')
        
        self.cursor.execute('DROP TABLE IF EXISTS equipment')
        self.cursor.execute('''CREATE TABLE equipment (
                        equipment_id INT,
                        name VARCHAR(20),
                        group_name VARCHAR(20)
                    )''')

    def insert_data_on_table(self, data, table):
        for row in data:
            campos = ', '.join(row.keys())
            valores = ', '.join(['%s'] * len(row))
            query = f"INSERT INTO {table} ({campos}) VALUES ({valores})"
            
            # Executar a query
            self.cursor.execute(query, list(row.values()))

        self.cnx.commit()

    def execute_query(self, query):
        self.cursor.execute(query)
        resultado = self.cursor.fetchone()
        return resultado
    
    def execute_query_all_results(self, query):
        self.cursor.execute(query)
        resultado = self.cursor.fetchall()
        return resultado