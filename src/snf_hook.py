import snowflake.connector

class Snf:
    
    def conn(conn_config : dict):
        return snowflake.connector.connect(**conn_config)
    
    def create_cursor(conn):
        return conn.cursor()
    
    #create staging table 
    def create_staging_table(cursor):
        cursor.execute(f"""
            CREATE OR REPLACE TABLE staging_table (
            id INTEGER, 
            data STRING,
            last_modified TIMESTAMP
        )
    """)
    
    #load data to staging table
    def load_data_staging_table(cursor, s3_path, aws_conn : dict):
        cursor.execute(f"""
        COPY INTO staging_table
        FROM '{s3_path}'
        CREDENTIALS=(AWS_KEY_ID='{aws_conn["aws_access_key_id"]}' AWS_SECRET_KEY='{aws_conn["aws_secret_access_key"]}')
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    """)
        
    #load data in merge mode
    def merge_data(cursor):
        cursor.execute(f"""
        MERGE INTO target_table AS target
        USING staging_table AS staging
        ON target.id = staging.id
        WHEN MATCHED THEN
            UPDATE SET target.data = staging.data, target.last_modified = staging.last_modified
        WHEN NOT MATCHED THEN
            INSERT (id, data, last_modified) VALUES (staging.id, staging.data, staging.last_modified)
    """)
        
    #truncate load if needed 
    def truncate_load(cursor, table_name, s3_path, aws_conn : dict):
        cursor.execute(f"TRUNCATE TABLE {table_name}")   
        cursor.execute(f"""
        COPY INTO {table_name}
        FROM '{s3_path}'
        CREDENTIALS=(AWS_KEY_ID='{aws_conn["aws_access_key_id"]}' AWS_SECRET_KEY='{aws_conn["aws_secret_access_key"]}')
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    """)

        
    def drop_staging_table(cursor):
        cursor.execute("DROP TABLE IF EXISTS staging_table")
    
    def close_conn(cursor, conn):
        cursor.close()
        conn.close()

        