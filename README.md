kafka2es作用
===================

订阅kafka集群的某个topic的数据到es中，并根据订阅的日志内容product，event_time创建索引，索引名为aqueducts_product_年-月-日，type由service决定

kafka2es使用
===================

编译命令：mvn -f pom.xml assembly:assembly

配置文件为.json文件，例如kafka2es-example.json:

    {
        "zkHost":"xxx", //zk的ip或者vip
        "esHost":"xxx",  //es集群的ip或者vip
        "esCluster":"es_test",    //es集群的名字
        "kafkaTopic":"xxx",   //订阅kafka的topic
        "kafkaGroup":"xxxx",  //订阅kafka的group
        "threadNum":"3",            //开启线程数
        "bulkMaxSize":"3000",       //批量创建索引的最大数据量
    }

运行命令：java -jar kafka2es kafka2es-example.json
