import argparse

import json
import logging
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io import ReadStringsFromPubSub, WriteToBigQuery, BigQuerySink
import math


if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--project", required=True, help="GCP Project id.")
    parser.add_argument("--topic", required=True, help="Topic of Pub/Sub to read from.")
    parser.add_argument("--dataset", required=True, help="BQ dataset to write table to.")
    parser.add_argument("--table", required=True, help="BQ table to write data on.")
    
    args, dataflow_args = parser.parse_known_args()

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    # pipeline_options.view_as(SetupOptions).streaming = True
    


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
            "fields": [
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
    p = beam.Pipeline(options=pipeline_options, argv=dataflow_args)
    messages = (p
                | "Read messages" >> ReadStringsFromPubSub(topic=args.topic)
                | "Decode and format json" >> beam.Map(lambda x: json.loads(x)))

    order_product = messages | "Extract id" >> beam.Map(lambda x: (x.get("order_id"), x))

    def group_products(order_products):
        order_id, products = order_products
        output = {"order_id": str(order_id), "product": []}
        logging.info("order_id: {}".format(str(order_id)))
        for product in products:
            output["device_id"] = product.pop("device_id")
            product.pop("order_id")
            output["product"] = output["product"] + [product]
        return output

    

    orders = (order_product 
              | beam.WindowInto(window.Sessions(500))
              | "Group by order" >> beam.GroupByKey()
              | "Join orders" >> beam.Map(group_products))
    

    # output = (orders
    #           | "Format orders" >> beam.Map(format_orders))

    orders |  WriteToBigQuery(args.table, args.dataset, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    p.run()
    
     

    


