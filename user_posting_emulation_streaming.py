import requests
from time import sleep
import random
from multiprocessing import Process
import json
import sqlalchemy
from sqlalchemy import text

random.seed(100)
class AWSDBConnector:
    """    
    This class can be used to connect to an AWS RDS database hosting Pinterest post data by using a SQLAlchemy engine.
    """
    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        """
        This function is used to initialise a SQLAlchemy database engine to interact with an AWS RDS database.

        Returns:
                engine (sqlalchemy.engine.Engine): Interface for interacting with database
        """
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()
def run_infinite_post_data_loop():
    """
    This function is used to emulate the relevant data being collected and sent to Kafka topics when users make post on Pinterest.
    
    At random intervals between 0 and 2 seconds a random row is selected from the three tables in the RDS database, representing 
    all of the data collected when a user makes a post on Pinterest (the same row on each table, representing the same post).

    The data is then reformatted and sent to three AWS Kinesis streams via an API.

    This process will run indefinitely until interrupted by the user
    """
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            # Collect data from the selected random row in each table by executing SQL queries
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
            
            # Invoke urls defined for each topic 
            invoke_url_list = [f"https://6etgk9qrli.execute-api.us-east-1.amazonaws.com/production/streams/streaming-0ad8a60ac12f-{x}/record" for x in ["pin", "geo", "user"]]
            # Data reformatted as required for AWS Kinesis
            formatted_results = [{"StreamName": f"streaming-0ad8a60ac12f-{x}", 
                                  "Data": result, 
                                  "PartitionKey": f"partition-{x}"} for x, result in zip(["pin", "geo", "user"], [pin_result, geo_result, user_result])]
    
            #print(formatted_results)
            # Each of the three formatted results sent to appropriate AWS Kinesis Data Streams via API PUT requests
            headers = {'Content-Type': 'application/json'}
            for i in range(3):
                response = requests.request("PUT", invoke_url_list[i], headers=headers, data=json.dumps(formatted_results[i], default=str))
                if response.status_code != 200:
                    print(f"Error: {response.status_code}")
                    print(f"Response Content: {response.content}")
                    #print(f"Payload: {payload})")
                else:
                    print(f"Response Content {i}: {response.content}")

            #print(pin_result)
            #print(geo_result)
            #print(user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()