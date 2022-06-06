docker run --rm -it \
    --network spark-network \
    --name spark-submit \
    -v $(pwd):/opt/app bitnami/spark:3 /bin/bash \
    -c "cd /opt/app && spark-submit --master spark://spark:7077 --deploy-mode client main.py"

