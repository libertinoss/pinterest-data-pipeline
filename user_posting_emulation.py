import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            invoke_url_list = [f"https://6etgk9qrli.execute-api.us-east-1.amazonaws.com/production/topics/0ad8a60ac12f.{x}" for x in ["pin", "geo", "user"]]
            #To send JSON messages you need to follow this structure
            formatted_results = [{"records":[{"value":result}]} for result in [pin_result, geo_result, user_result]]
            print(formatted_results)
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            for i in range(3):
                response = requests.request("POST", invoke_url_list[i], headers=headers, data=json.dumps(formatted_results[i], default=str))
                if response.status_code != 200:
                    print(f"Error: {response.status_code}")
                    print(f"Response Content: {response.content}")
                    #print(f"Payload: {payload})")

            print(response.text)
            
            #print(pin_result)
            #print(geo_result)
            #print(user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


