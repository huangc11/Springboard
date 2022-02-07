import mysql.connector
import csv
import datetime as dt



def get_db_connection(username, password,host , port, database):
    connection = None
    try:
        connection = mysql.connector.connect(user=username,
                                             password=password,
                                             host=host,
                                             port=port,
                                             database=database)
        print('successfully connected.')
    except Exception as error:
        print(error)
        print("Error while connecting to database for job tracker", error)
    return connection


def format_row(row):
    def f_to_date(str='2020-08-01'):
        """ convert date formatted string to date"""
        return dt.datetime.strptime(str, '%Y-%m-%d')

    def f_to_date_int(str='2020-08-01'):
        """ convert date string to integer"""
        return int(f_to_date(str).strftime("%Y%m%d"))

    new_row = (
    int(row[0]), int(f_to_date_int(row[1])), int(row[2]), row[3], f_to_date(row[4]), row[5], row[6], int(row[7]),
    float(row[8]), int(row[9]))
    return new_row


def extract_from_csv(connection, file_path_csv):
    cursor = connection.cursor()
    # [Iterate through the CSV file and execute insert statement]


    with open(file_path_csv) as fh:
        reader = csv.reader(fh)
        for row_raw in reader:
            # format this row
            row = format_row(row_raw)

            # insert into database
            insert_stmt = "INSERT INTO sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
            cursor.execute(insert_stmt, values)

        connection.commit()

    cursor.close()


def query_popular_tickets(connection):
    # Get the most popular ticket in the past month
    sql_statement = 'select event_name, sum(num_tickets) as total_tickets from sales group by event_name order by 2 desc limit 2;'
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records

def print_msg(msg):
    print('---- '+ msg)



if __name__ == "__main__":

    try:

        username = input('Please enter the databse username:') or 'chu'
        password = input('Please enter the databse password:') or 'tree'
        #port = int(input('please enter the port:')) or 3306
        port =int(input('please enter the port(default=3306):') or '3306')
        host = input('Please enter the localhost(default="localhost"):') or 'localhost'
        database = input('Please enter the databse(default=\'ticket_system\'):') or 'ticket_system'
        #connect to database
        print_msg('connecting to database at {}, {} '.format(host, port, database, username) )
        connection=get_db_connection(username, password, host, port, database)
        print("")

      #  print_msg('Connected to database')
        #extract, format and load

        print_msg('Data loading process started...')
        #rows= extract_from_csv(connection,'proj15_sales.csv')
        rows = extract_from_csv(connection, 'third_party_sales_1.csv')
        print_msg('Data loaded to database....')
        print("")

        #run squry

        records=query_popular_tickets(connection)
        print_msg('Analysys completed.')
        print("Here are the top 2 most popular tickets:")

        #print result
        for record in records:
            print('- {}({})'.format(record[0],record[1]))


    except:
        print('There were errors.  Process aborted')