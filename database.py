import mysql.connector
import mysql
import time

class database():
    def __init__(self):
        self.isConnect = False
        self.dbHost = 'snf-876565.vm.okeanos.grnet.gr'
        self.dbName = 'testcrawler'
        self.dbUser = 'theofanis'
        self.dbPass = '9.^9#M<4*k7tN%d,'


    def connect(self):
        try:
            self.connection = mysql.connector.connect(host=self.dbHost, database=self.dbName, user=self.dbUser,password=self.dbPass)
            if self.connection.is_connected():
                self.isConnect = True
        except:
            self.isConnect = False

    def close(self):
        self.connection.close()

    def checkConnection(self):
        return self.isConnect

    def query(self, query):
        if (self.isConnect is False):
            self.connect()
        cursor = self.connection.cursor()
        self.connection.commit()
        cursor.execute(query)
        records = cursor.fetchone()
        return records[0]

    def query_all(self, query):
        if(self.isConnect is False):
            self.connect()
        cursor = self.connection.cursor()
        self.connection.commit()
        cursor.execute(query)
        records = cursor.fetchall()
        return records[0]

    def query_all_d(self, query):
        if (self.isConnect is False):
            self.connect()
        cursor = self.connection.cursor()
        self.connection.commit()
        cursor.execute(query)
        records = cursor.fetchall()
        return records
