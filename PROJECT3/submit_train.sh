#!/usr/bin/env bash
# submit_train.sh
# Run from the project3/ directory in PyCharm terminal
docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2G \
    --executor-memory 4G \
    /app/train_model.py
