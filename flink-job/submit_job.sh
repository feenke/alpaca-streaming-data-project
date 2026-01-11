#!/bin/bash

echo "Waiting for JobManager to be ready..."
sleep 20

echo "Submitting PyFlink job to cluster..."
/opt/flink/bin/flink run \
    --jobmanager jobmanager:8081 \
    --python /opt/flink-job/aggregation_job.py

echo "Job submission completed."