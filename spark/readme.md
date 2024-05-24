
# Install

```sheel
sudo chmod +x install.sh

./install.sh


# spark cluster with 3 workers
docker-compose -p spark up -d --scale spark-worker=3

# Start the history server:
docker exec spark_spark_1 start-history-server.sh

# submit the main.py 
## workload q1 - sem partição manual
docker exec spark_spark_1 python3 main.py w1

# stop
docker-compose -p spark stop

# delete
docker-compose -p spark down

```

```
Acesso ao Spark History Server
http://35.192.163.177:28080/
http://34.72.114.223:28080/
```