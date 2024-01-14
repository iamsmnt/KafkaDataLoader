import json
from kafka import KafkaConsumer
from db import postgresql_db
import pandas as pd 

def main():
    consumer = KafkaConsumer('test-topic')
    for msg in consumer:
        req_json = json.loads(msg.value)
        host = req_json['db_params']['host']
        port = req_json['db_params']['port']
        database = req_json['db_params']['db']
        user = req_json['db_params']['user']
        password = req_json['db_params']['password']
        table_name = req_json['db_params']['table_name']
        schema_name = req_json['db_params']['schema_name']

        pg_manager = postgresql_db.PostgreSQLManager(host=host,port=port,database=database,user=user,password=password)
        if pg_manager.check_connection_status():
            connection_engine = pg_manager.get_sqlalchemy_engine()
            df = pd.read_csv('/Users/somnathmahato/backend-projects/kafka-test/kafka_api/annual-enterprise-survey-2021-financial-year-provisional-csv.csv')
            df_ = df.head(10)
            df_.to_sql(name=table_name,con=connection_engine,if_exists='replace')
            print("Database connectivity is active.")
        else:
            print("Unable to connect to the database.")
    print('Consumer Called!')
        

if __name__ == "__main__":
    while True:
        try:
            print('Consumer Running!')
            main()
        except KeyboardInterrupt:
            print('Consumer Stopped!')
            break