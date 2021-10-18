from database import *
from deduplication import *
import string
import re

class main_deduplication:
    conn = mysql.connector.connect(host="snf-876565.vm.okeanos.grnet.gr",
                                   port="3306",
                                   user="theofanis",
                                   database="testcrawler",
                                   password="9.^9#M<4*k7tN%d,")
    cursor = conn.cursor()

    def __init__(self):
        self.data_base = database()
        self.deduplication_ = deduplication()


    #check option id next id to process
    def id(self):
        sql_id_deduplication_option = 'SELECT `value` FROM `options` WHERE `id` = 7'
        next_id = self.data_base.query(sql_id_deduplication_option)
        return next_id

    def data_id(self,id):
        sql_id_deduplication_option = 'SELECT `job_url`,`job_description` FROM `collumns` WHERE `id` = "' + str(id)+'"'
        next_id = self.data_base.query_all(sql_id_deduplication_option)
        return next_id

    def data_clean(self,data):

        print(data)
        title = re.search(r"title\" : \"(.*?)\"", str(data[1])).group()
        description = re.search(r"description\" : \"(.*?)\"", str(data[1])).group()
        employ_type = re.search(r"employmentType\" : \"(.*?)\"",  str(data[1])).group()
        company_name = re.search(r"name\" : \"(.*?)\"",  str(data[1])).group()
        address_country = re.search(r"addressCountry\": \"(.*?)\"",  str(data[1])).group()
        address_region = re.search(r"addressRegion\": \"(.*?)\"", str(data[1])).group()
        address_locality = re.search(r"addressLocality\": \"(.*?)\"",  str(data[1])).group()
        address_street = re.search(r"streetAddress\": \"(.*?)\"", str(data[1])).group()
        date_posted = re.search(r"\d{4}-\d{2}-\d{2}", str(data[1])).group()
        date_end = re.search(r"(?<=[validThrough : ])?\d{4}-\d{2}-\d{2}(?=[T])", str(data[1])).group()

        print(title[10:len(title)-1])
        print(description[16:len(description)-1])
        print(employ_type[19:len(employ_type)-1])
        print(company_name[9:len(company_name)-1])
        print(address_country[18:len(address_country)-1])
        print(address_region[17:len(address_region)-1])
        print(address_locality[21:len(address_locality)-1])
        print(address_street[17:len(address_street)-1])
        print(date_posted)
        print(date_end)
        print("count is:"+" " +id )

        try:

            conn = mysql.connector.connect(host="snf-876565.vm.okeanos.grnet.gr",
                                           port="3306",
                                           user="theofanis",
                                           database="testcrawler",
                                           password="9.^9#M<4*k7tN%d,")
            cursor = conn.cursor(prepared=True)
            sql_insert_query = """INSERT INTO
                                                    `tester`(
                                                      `title`,
                                                      `description`,
                                                      `employ_type`,
                                                      `company_name`,
                                                      `address_country`,
                                                      `address_region`,
                                                      `address_locality`,
                                                      `address_street`,
                                                      `date_posted`,
                                                      `date_end`
                                                      )VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            insert_tuple = (
                title[10:len(title)-1], description[16:len(description)-1], employ_type[19:len(employ_type)-1], company_name[9:len(company_name)-1],address_country[18:len(address_country)-1],
                address_region[17:len(address_region)-1], address_street[17:len(address_street)-1],
                address_locality[21:len(address_locality)-1], date_posted, date_end)
            result = self.cursor.execute(sql_insert_query, insert_tuple)

            sql_id_deduplication_option = self.cursor.execute(
                'UPDATE `options` SET `value` = `value` +1 WHERE `id` = 7')

            self.conn.commit()
        except mysql.connector.Error as error:
            self.conn.rollback()
            print("Failed to insert into MySQL table{}".format(error))




if __name__ == "__main__":
    while True:
        run_app = main_deduplication()
        id = run_app.id()
        data = run_app.data_id(id)
        data_clean = run_app.data_clean(data)





