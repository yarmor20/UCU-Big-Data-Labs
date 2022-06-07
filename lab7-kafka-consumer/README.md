# Lab 7: Reading from Kafka
**Author**: Yaroslav Morozevych
### Run
1. Start Zookeeper and Broker. Create a topic.
```bash
lab7/scripts $ sh run-cluster.sh 
lab7/scripts $ sh create-topic.sh 
```
2. Start Producer.
```bash
lab7/producer $ sh deploy-producer.sh 
lab7/scripts $ sh start-producer.sh 
```
3. Start Consumer.
```bash
lab7/consumer $ mkdir results
lab7/consumer $ sh deploy-consumer.sh 
lab7/scripts $ sh start-consumer.sh 
```
4. Shutdown Cluster.
```bash
lab7/scripts $ sh shutdown-cluster.sh 
```

### Results
- The output files are located in `lab7/consumer/results` directory.
- Screenshots are located in `lab7/screenshots`.