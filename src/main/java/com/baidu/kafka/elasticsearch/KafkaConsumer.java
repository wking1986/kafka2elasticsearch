package com.baidu.kafka.elasticsearch;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;
import java.util.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.FileReader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.baidu.kafka.elasticsearch.ConfigFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumer {
  private ConsumerConnector consumer;
  private String topic;
  private ExecutorService executor;
  private Properties props = new Properties();
  private KafkaStream stream;
  private String esHost;
  private int bulkSize;
  private String esCluster;
  private List<KafkaStream<byte[], byte[]>> streams;
  protected static Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class); 

  public KafkaConsumer(String zookeeper, String groupId, String topic, String esHost, int bulkSize, String esCluster) {
    LOG.info( "kafka init" );
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
    this.topic = topic;
    this.esHost = esHost;
    this.bulkSize = bulkSize;
    this.esCluster = esCluster;
  }

  public void shutdown() {
    if (consumer != null) consumer.shutdown();
    if (executor != null) executor.shutdown();
  }

  public ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
    //config zookeeper 
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", ConfigFile.ZK_SESSION_TIMEOUT_MS);
    props.put("zookeeper.sync.time.ms", ConfigFile.ZK_SYNC_TIME_MS);
    props.put("auto.commit.interval.ms", ConfigFile.AUTO_COMMIT_INTERVAL_MS);
    props.put("consumer.timeout.ms", ConfigFile.CONSUMER_TIMEOUT_MS);
    props.put("auto.offset.reset", ConfigFile.AUTO_OFFSET_RESET);
    props.put("rebalance.backoff.ms", ConfigFile.REBALANCE_MS);
    ConsumerConfig config = new ConsumerConfig(props);
    return config;
  }

  public void run(int numThreads) {
    LOG.info( "Run!" );
    //subscription kafka topic
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(numThreads));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
    //stream = consumerMap.get(topic).get(0);
    //multithreading elasticsearch index
    executor = Executors.newFixedThreadPool(new Integer(numThreads));
    int threadNumber = 0;
    for(KafkaStream<?, ?> stream : streams) {
        executor.submit(new ElasticsearchInsert(stream, esHost, threadNumber, bulkSize, esCluster));
	threadNumber++;
    }
  }

  public static void main(String[] args) {
    try {
	//read json config
	String jsonPath = args[0];
        JSONParser parser = new JSONParser();
	Object obj = parser.parse(new FileReader(jsonPath));
	JSONObject jsonObject = (JSONObject) obj;
	String zkHost = (String)jsonObject.get("zkHost");
        String zkHostPort = zkHost + ":" + ConfigFile.ZK_PORT;
	String esHost = (String)jsonObject.get("esHost");
	String topic = (String)jsonObject.get("kafkaTopic");
	String groupId = (String)jsonObject.get("kafkaGroup");
	int threads = Integer.parseInt((String)jsonObject.get("threadNum"));
        int bulksize = Integer.parseInt((String)jsonObject.get("bulkMaxSize"));
	String esCluster = (String)jsonObject.get("esCluster");
        LOG.info( "Start!" );
        LOG.info( "topic is: "+topic );
	//create kafka consumer
	KafkaConsumer kComsumer = new KafkaConsumer(zkHostPort, groupId, topic, esHost, bulksize, esCluster);
	//run elasticsearch index
	kComsumer.run(threads);
	LOG.info( "End!" );
    } catch (Exception e) {
	LOG.info( "{} " + e.getMessage());
    }
  }
}
