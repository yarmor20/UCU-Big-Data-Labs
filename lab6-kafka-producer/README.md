# Lab 6: Writing to Kafka
**Author**: Yaroslav Morozevych
### Run
1. Start Zookeeper and Broker. Create a topic.
```bash
lab6/scripts $ sh run-cluster.sh 
lab6/scripts $ sh create-topic.sh 
```
2. Start Producer.
```bash
lab6 $ sh deploy-producer.sh 
lab6/scripts $ sh start-producer.sh 
```
3. Start Consumer.
```bash
lab6/scripts $ sh start-consumer.sh 
```
4. Shutdown Cluster.
```bash
lab7/scripts $ sh shutdown-cluster.sh 
```

### Results
- Screenshots are located in `lab6/screenshots`.