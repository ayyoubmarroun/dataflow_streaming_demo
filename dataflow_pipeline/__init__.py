import argparse

import json
import logging
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io import ReadStringsFromPubSub, WriteToBigQuery


if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--topci", required=True, help="Topic of Pub/Sub to read from.")
    args = parser.parse_args()

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(SetupOptions).streaming = True


    schema = [
        {
            "name": "order_id",
            "type": "STRING"
        }, {
            "name": "device_id",
            "type": "STRING"
        }, {
            "name": "timestamp",
            "type": "TIMESTAMP"
        }, {
            "name": "product",
            "type": "RECORD",
            "mode": "REPEATED",
            "fileds": [
                {
                    "name": "id",
                    "type": "STRING"
                }, {
                    "name": "name",
                    "type": "STRING"
                }, {
                    "name": "brand",
                    "type": "STRING"
                }, {
                    "name": "price",
                    "type": "FLOAT"
                }, {
                    "name": "quantity",
                    "type": "INTEGER"
                }
            ]
        }
    ]
    p = beam.Pipeline(options=pipeline_options)
    messages = (p
                | "Read messages" >> ReadStringsFromPubSub(topic=args.topic)
                .with_output_types(bytes) 
                | "Decode and format json" >> beam.Map(lambda x: json.dumps(x.decode("utf-8"))))

    order_product = messages | "Extract id" >> beam.Map(lambda x: (x.get("order_id"), x))

    def group_products(order_products):
        pass

    orders = (order_product 
              | beam.WindowInto(window.Sessions(300))
              | "Group by order" >> beam.GroupByKey()
              | "Join orders" >> beam.Map(group_products))
    
    def format_orders(orders):
        pass

    output = (orders
              | "Format orders" >> beam.Map(format_orders))

    output | WriteToBigQuery(args.table, args.dataset, schema=schema)

    p.run().waint_until_finish()
    
     

    


