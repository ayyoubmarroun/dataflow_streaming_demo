#! /bin/sh
python -m dataflow_pipeline --runner "DataflowRunner" \
                   --jobName "dataflow-pipeline-demo" \
                   --topic "projects/$1/topics/$2" \
                   --project "$1" \
                   --dataset "ETL" \
                   --table "dataflow_stream" \
                   --staging_location "gs://stagging-dataflow/stagging/" \
                   --temp_location "gs://stagging-dataflow/tmp/" \
                   --streaming