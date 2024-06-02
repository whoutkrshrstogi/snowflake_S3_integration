from s3_hook import S3
from snf_hook import Snf
from connection import s3_conn, snf_conn


if __name__ == "__main__":
    snf_conn = snf_conn
    aws_conn = s3_conn
    s3_path = ""
    s3_key = ""
    bucket_name = ""
    previous_time = ""

    s3_client = S3.get_s3_client(s3_conn)
    latest_timestamp = S3.get_s3_last_modified(clinet= s3_client, bucket_name=bucket_name, s3_key=s3_key )
    if S3.verify_lates_file(previous_time = previous_time, s3_latest = latest_timestamp):
        try:
            #setup connection
            conn  = Snf.conn(snf_conn)
            #create the cursor
            cursor = Snf.create_cursor(conn=conn)
            #create staging table
            Snf.create_staging_table(cursor=cursor)
            #load data to staging table
            Snf.load_data_staging_table(cursor=cursor, s3_path=s3_path, aws_conn=aws_conn )
            #load to target table using merge 
            Snf.merge_data(cursor=cursor)

        except Exception as e:
            raise e
        finally:
            #drop the table and close the connection
            Snf.drop_staging_table(cursor=cursor)
            Snf.close_conn(conn=conn, cursor=cursor)


