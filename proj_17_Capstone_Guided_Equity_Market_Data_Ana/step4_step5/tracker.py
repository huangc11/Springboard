import datetime
import psycopg2

from dbconfig import dbConfig

class Tracker(object):
    """
    job_id, status, updated_time
    """
    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig


    def assign_job_id(self):
        #[Construct the job ID and assign to the return variable]

        curr = datetime.datetime.now()

        dt_str = str(curr.year) + str(curr.month) + str(curr.day) + str(curr.hour) + str(curr.minute)\
                 + str(curr.second)

        self.job_id = self.jobname + '_'+ dt_str
        return self.job_id

    def update_job_status(self, status):
        job_id = self.assign_job_id()
        print("Job ID Assigned: {}".format(job_id))
        update_time = datetime.datetime.now()
        #gable_name = self.dbconfig.get("postgres", "job_tracker_table_name")
        connection = self.get_db_connection()
        try:

            cursor = connection.cursor()

            sql = """INSERT INTO  job_status(job_id, status, updated_time) VALUES(%s, %s, %s) """
            cursor.execute(sql, (self.job_id, status,datetime.datetime.now()))

            connection.commit()

        except  Exception  as e:
            print("error executing db statement for job tracker.")
            print(e)
        return


    def get_job_status(self, job_id):

        # connect db and send sql query
        table_name = self.dbconfig.get('postgres', 'job_tracker_table_name')
        connection = self.get_db_connection()
        try:
            record = 1
            # [Execute SQL query to get the record]
            return record
        except Exception as e:
                #(Exception, psycopg2.Error) as error:
             print("error executing db statement for job tracker.")
             print(e)
        return


    def get_db_connection(self):
        connection = None

        try:
            connection = psycopg2.connect(
                user=self.dbconfig.user,
                password=self.dbconfig.password,
                host=self.dbconfig.host,
                port=self.dbconfig.port,
                database=self.dbconfig.database
            )

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
        return connection





if __name__=='__main__':


    dbconfig = dbConfig("cuser2", "cuser2","localhost", "5432","capstone2")

    tracker=Tracker("EOD", dbconfig)
    job_id = tracker.assign_job_id()
    print(job_id)
    tracker.update_job_status('end')

    tracker.update_job_status('start')

