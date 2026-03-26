#!/usr/bin/env bash
# submit_predict.sh
# Run from the project3/ directory in PyCharm terminal
docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2G \
    --executor-memory 4G \
    /app/predict.py
