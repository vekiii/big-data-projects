# Big Data Project 1 - Vital Signs Analysis

This project implements a complete Big Data pipeline using **Hadoop HDFS** for storage and **Apache Spark** for distributed processing. The analysis is performed on the University of Queensland Vital Signs Dataset (~21 million rows).
Link to the data: [https://outbox.eait.uq.edu.au/uqdliu3/uqvitalsignsdataset/index.html](http://dx.doi.org/102.100.100/6914)

## 📂 Repository Structure
* **`prepare_data.py`**: ETL script used to merge and clean multiple CSV folders into a unified dataset.
* **`spark_analysis.py`**: Diagnostic tool for data verification.
* **`spark_final.py`**: Main application that handles task-specific filtering, sorting, and statistical calculations.
* **`docker-compose.yml`**: Full cluster configuration (Master, Worker, NameNode, DataNode).

## 🚀 Performance Tuning
To handle the large dataset volume (~4.14 GB in HDFS), the cluster was optimized:
* **Spark Worker Memory**: 8GB
* **Spark Worker Cores**: 4
* **Driver/Executor Memory**: Configured during submission to 2GB/4GB to prevent OOM errors during global sorting.

## 📊 Results
### Task 1: Filtering and Sorting
Filters data based on user input (e.g., HR > 100) and sorts by a specific column (e.g., SpO2).

### Task 2: Advanced Statistics
Calculates Min, Max, Mean, Std Dev, Variance, and Median for patient cases.

## 🛠️ How to Run
```powershell
docker exec -it spark-master /spark/bin/spark-submit --driver-memory 2G --executor-memory 4G /app/spark_final.py --task 2 --col HR
