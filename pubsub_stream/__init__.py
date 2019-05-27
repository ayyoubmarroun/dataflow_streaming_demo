"""Streaming module."""

import json
from google.cloud import pubsub_v1
import time
import pandas as pd
import random
import os
import uuid
dir_ = os.path.abspath(os.path.dirname(__file__))

class PubSubStreamer(object):
    """Pubsub streaming class."""

    def __init__(self, project_id, topic, speed=1):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic = self.publisher.topic_path(project_id, topic)
        self.device_id = str(os.getpid())
        self.speed = speed
        products = pd.read_csv(os.path.join(dir_, "products.csv"))
        self.products = products.filter(["uniq_id", "product_name","brand",  "retail_price"])
        self.products.columns = ["id", "name", "brand", "price"]
        self.products["price"] = self.products["price"][self.products.price.isna()] = 0
        self.products["brand"] = self.products["brand"][self.products.brand.isna()] = "n/a"

    def __call__(self):
        
        while True:
            message = self.generate_message()
            message_future = self.publisher.publish(self.topic, message, device_id=self.device_id, timestamp=str(time.time()))
            message_future.add_done_callback(self.callback)
            time.sleep(1/self.speed)

    @staticmethod
    def callback(message_future):
        if message_future.exception(timeout=30):
            print('Publishing message threw an Exception {}.'.format(
            message_future.exception()))
        else:
            print(message_future.result())

    
    def generate_sample_order(self):
        sample = self.products.sample(n=random.randint(1,10))
        self.order_id = str(uuid.uuid4())
        self.order_iterator = sample.iterrows()

    def generate_message(self):
        try:
            if not getattr(self, "order_iterator", None):
                self.generate_sample_order()
            content = next(self.order_iterator)
            return self.format_message(content[1])
        except StopIteration:
            self.generate_sample_order()
            content = next(self.order_iterator)
            return self.format_message(content[1])

    
    def format_message(self, content):
        message = {
            "order_id": self.order_id,
            "device_id": self.device_id, 
            "quantity": random.randint(1,5)
        }
        message.update(content.to_dict())
        return json.dumps(message)
        


if __name__ == '__main__':
    print("hello world")
    