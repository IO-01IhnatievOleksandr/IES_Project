import json
import logging
from typing import List
import pydantic_core
import requests
from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_api_gateway import StoreGateway

class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url
    

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        # Make a POST request to the Store API endpoint with the processed data
        data = [json.loads(item.json()) for item in processed_agent_data_batch]
        print(data)
        test = requests.post(f"{self.api_base_url}/processed_agent_data/", json = data)
        if test.status_code == 200:       
            return True
        else:
            return False