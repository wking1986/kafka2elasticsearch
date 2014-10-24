kafka2elasticsearch
===================

Subscribe to the Kafka cluster of a topic data into es, and according to the subscription log content product, event_time create index index name is aqueducts_product_ year month day, type determined by service

编译：mvn -f pom.xml assembly:assembly

升级之后的kafka2es-0.2实现可配置多线程订阅kafka数据和批量创建es索引，配置文件为.json文件，例如kafka2es-0.2-example.json:

    {
        "zkHost":"xxx", //zk的ip或者vip
        "esHost":"xxx",  //es集群的ip或者vip
        "esCluster":"es_test",    //es集群的名字
        "kafkaTopic":"xxx",   //订阅kafka的topic
        "kafkaGroup":"xxxx",  //订阅kafka的group
        "threadNum":"3",            //开启线程数
        "bulkMaxSize":"3000",       //批量创建索引的最大数据量
    }

kafka2es-0.2运行：java -jar kafka2es-0.2 kafka2es-0.2-example.json
