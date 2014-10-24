package com.baidu.kafka.elasticsearch;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Date;
import java.lang.Double;
import java.lang.Long;
import java.lang.String;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.Integer;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import static org.elasticsearch.node.NodeBuilder.*;

import org.json.JSONObject;
import com.baidu.kafka.elasticsearch.ConfigFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class ElasticsearchInsert implements Runnable {
  private int threadNum;
  private int bulkSize;
  private KafkaStream stream;
  private int threadNumber;
  private String kafkaMsg;
  protected String productName;
  protected String serviceName;
  protected TransportClient client;
  protected String indexName;
  protected String typeName;
  protected ArrayList<JSONObject> jsonList = new ArrayList<JSONObject>();
  protected Map<String,NodeInfo> nodesMap = new HashMap<>();
  protected String elasticSearchHost = ConfigFile.ES_HOSTS;
  protected Integer elasticSearchPort = ConfigFile.ES_PORT;
  protected String elasticSearchCluster = ConfigFile.ES_CLUSTER_NAME;
  protected static Logger LOG = LoggerFactory.getLogger(ElasticsearchInsert.class);

  public ElasticsearchInsert(KafkaStream stream, String esHost, int threadNum, int bulkSize, String esCluster) {
    this.stream = stream;
    this.threadNum = threadNum;
    this.bulkSize = bulkSize;
    elasticSearchCluster = esCluster;
    elasticSearchHost = esHost;
    //initialization elasticsearch with TransportClient
    Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", elasticSearchCluster).build();
    client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(elasticSearchHost, elasticSearchPort));
    NodesInfoResponse response = client.admin().cluster().nodesInfo(new NodesInfoRequest().timeout("60")).actionGet();
    nodesMap = response.getNodesMap();
    for(String k: nodesMap.keySet()){
    if(!elasticSearchHost.equals(nodesMap.get(k).getHostname())) {
       client.addTransportAddress(new InetSocketTransportAddress(nodesMap.get(k).getHostname(), elasticSearchPort));
      }
    }
  }
  //batch index and insert elastcisearch
  public boolean insertES(ArrayList<JSONObject> jsonAry) {
    String document = null;
    try {
	 BulkRequestBuilder bulkRequest = client.prepareBulk();
         for(JSONObject json: jsonAry) {
             productName = json.getString("product");
             serviceName = json.getString("service");
             long insertTime = System.currentTimeMillis();
             long eventTime = json.getLong("event_time");
             //long cacheTime = json.getLong("cache_time");
             json.put("insert_time", insertTime);
             SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
             String dateTime = sf.format(eventTime);
             String dateTimeYMD[] = dateTime.split(" ");
             indexName = "aqueducts_" + productName + "_" + dateTimeYMD[0];
             typeName = serviceName;
             document = json.toString();
             bulkRequest.add(this.client.prepareIndex(indexName, typeName).setSource(document));
	 }
         BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
         return true;
    } catch(Exception e) {
         LOG.info("Unable to index Document[ " + document + "], Type[" + typeName + "], Index[" + indexName + "]", e);
         return false;
    }
  }
  
  public void run() {
    LOG.info("Inert ElasticSearch");
    LOG.info("thread num: " + threadNum);
    while(true) {
         ConsumerIterator<byte[], byte[]> msgStream = stream.iterator();
         long startTime = System.currentTimeMillis()/1000;
         int countPv = 0;
         try {
           while(msgStream.hasNext()) {
               //System.out.println("kafka msg-----");
	       //get kafka message
               kafkaMsg = new String(msgStream.next().message(), "UTF-8");
               JSONObject json = new JSONObject(kafkaMsg);
               jsonList.add(json);
               /*
               for(String k : nodesMap.keySet()){
                  LOG.info(k + ":" + nodesMap.get(k).getHostname());
               }
               jsonList.add(json.toString());
               */
	       int listSize = jsonList.size();
               long endTime = System.currentTimeMillis()/1000;
               countPv++;        //record index number
               if(((endTime-startTime) == ConfigFile.INDEX_INTERVAL) || listSize >= bulkSize) break;
           }
           if(insertES(jsonList)) {
              LOG.info("pv: " + countPv);
              countPv = 0;
	      jsonList.clear();
	   }
         } catch (Exception e) {
	   LOG.info("failed to construct index request "+ e.getMessage());
         }
    }
  }
}
