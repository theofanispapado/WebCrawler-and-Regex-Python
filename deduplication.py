from database import *
from main import *
import time
import datetime

class deduplication():
    def __init__(self):
        self.data_base = database()
    def time_fix(self,temp_data,current_data):
        if len(temp_data) >= 10 :
            temp_data = temp_data[0:10]
        else :
            temp_data = '2019-09-08'
        if len(current_data) >= 11:
            current_data = current_data[0:10]
        else:
            current_data = '2019-09-08'
        return temp_data,current_data
    def query_data(self,data,id):
        #sql_deduplication = 'SELECT * FROM `jobs` WHERE `title` = "%' + str(data[3]) + '%"  AND `city` = "%' + str(data[5]) + '%"'
        #sql_deduplication = 'SELECT `id`, `url`, `date_posted`, `date_end`,`title`, `company`, `city` FROM `jobs` WHERE `title` LIKE "%'+str(data[3])+'%"  AND  `city` LIKE  "%'+str(data[5])+'%" AND NOT `ID` LIKE  "'+str(id)+'"'
        sql_deduplication = 'SELECT  `date_posted`, `date_end`,`title`, `company_name`, `address_region`  FROM `tester` WHERE `title` LIKE "%'+(data[0])+'%" AND `company_name` LIKE "%'+(data[0])+'%" AND NOT `ID` LIKE  "'+str(id)+'"'
        print(sql_deduplication)
        query_data_deduplicate = self.data_base.query_all_d(sql_deduplication)

        return query_data_deduplicate

    def numOfDays(date1, date2):
        return (date2 - date1).days
    def date_check(self,data_exported,current_data,id):
        for i in range(len(data_exported)):
            temp_data= data_exported[i]
            if str(temp_data[3]) == '-1':
                if len(temp_data[2]) >= 10:
                    temp_data_date = temp_data[0:10]
                else:
                    if len(temp_data[2]) >= 10:
                        temp_data = temp_data[3]
                        temp_data = temp_data[0:10]
                        temp_data_date = datetime.datetime.strptime(str(temp_data), "%Y-%m-%d")
                        temp_data_date = temp_data_date + datetime.timedelta(days=30)
            else:
                if len(temp_data[3]) >= 10:
                    temp_data = temp_data[0:10]
                    temp_data_date = temp_data[3]
            deduplication_class=deduplication()
            print(temp_data_date ,current_data )
            temp_data,current_data = deduplication_class.time_fix(str(temp_data_date),str(current_data[1]))
            temp_data_date = datetime.datetime.strptime(str(temp_data_date), "%Y-%m-%d")
            current_data_date = datetime.datetime.strptime(str(current_data), "%Y-%m-%d")
            print(deduplication.numOfDays(current_data_date, temp_data_date), "days")
            #temp_data_date = datetime.datetime.timestamp(temp_data_date)
            #current_data_date = datetime.datetime.timestamp(current_data_date)
            #print(temp_data_date,current_data_date)
