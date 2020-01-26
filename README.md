# flume-kafka-storm
* 介绍
    1. 这是一个基于gradle的jar包管理, flume1.9.0, kafka1.1.1, storm1.2.3的简单的整合小工程
    2. 实现通过Cli类生成log数据，往flume agent写入数据，而flume将数据导入kafka，
        而storm从kafka拉取数据，过滤得到含有ERROR关键字的log数据，然后将这些数据
        第一写入特定文件，第二再将其导入kafka，由kafka的console consumer来消费掉。
* 运行
    1. 将本工程打成jar包上传至目标机器
        * 执行: storm jar fks.jar com.zq.main.LogFilterTopology fks 
                --artifacts "org.apache.storm:storm-kafka-client:1.2.3,org.apache.kafka:kafka_2.12:1.1.1" 
                --artifactRepositories "AliRepo1^http://maven.aliyun.com/nexus/content/groups/public/,AliRepo2^http://maven.aliyun.com/nexus/content/repositories/jcenter"
    2. 在本地运行Cli.main方法生成logs数据并上传至flume agent
* 详细示例见
    * https://github.com/ZhangQi1996/study_note/blob/master/big_data/storm/flume-kafka-storm.md        
