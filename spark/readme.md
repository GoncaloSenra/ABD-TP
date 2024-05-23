
# Install

```sheel
sudo chmod +x install.sh

./install.sh


# spark cluster with 3 workers
docker-compose -p spark up -d --scale spark-worker=3

# Start the history server:
docker exec spark_spark_1 start-history-server.sh

# submit the main.py job
docker exec spark_spark_1 python3 main.py

```

