package com.zq.main;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class LogFilterTopology {
    static class FilterBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String key = input.getStringByField("key");
            String line = input.getStringByField("all-logs");
            System.err.println("Accept: " + line);
            // 过滤掉不包含ERROR的行
            if (line.matches(".*(ERRORS?|errors?|errs?|ERRS?).*")) {
                collector.emit(new Values(key, line));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "filtered-logs"));
        }
    }

    static class StoredInFileBolt extends BaseRichBolt {

        private static final String FILTERED_LOGS_FILE = "/root/filtered-logs.txt";

        private Writer writer;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            try {
                writer = new FileWriter(FILTERED_LOGS_FILE, true);
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

        @Override
        public void execute(Tuple input) {
            try {
                String filteredLog = input.getStringByField("filtered-logs");
                writer.write(filteredLog);
                writer.flush();
                System.err.println(filteredLog);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    static class MyRecordTranslator implements Func<ConsumerRecord<String, String>, List<Object>> {
        // 原封不动返回value
        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.key(), record.value());
        }
    }

    public static void main(String[] args) throws Exception {
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig
                .builder("master:9092,slave1:9092", "flume-kafka-storm") // 你的kafka集群地址和topic
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "consumer") // 设置消费者组，随便写
                .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024 * 4) // 默认1m
                // .setRecordTranslator(new MyRecordTranslator())
                .setRecordTranslator( // 翻译函数，就是将消息过滤下，具体操作自己玩
                        new MyRecordTranslator(),
                        new Fields("key", "all-logs")
                )
//                .setRetry( // 某条消息处理失败的策略
//                        new KafkaSpoutRetryExponentialBackoff(
//                                new KafkaSpoutRetryExponentialBackoff.TimeInterval(500L, TimeUnit.MICROSECONDS),
//                                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2),
//                                Integer.MAX_VALUE,
//                                KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10)
//                        )
//                )
                .setOffsetCommitPeriodMs(10000)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST) // 最新开始拉取
                .setMaxUncommittedOffsets(250)
                .build();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaSpout<>(conf), 1);
        builder.setBolt("FilterBolt", new FilterBolt(), 2).shuffleGrouping("KafkaSpout");
        // 将过滤的数据一副本写入文件，一副本写入kafka的filtered-logs主题中
        Properties producerProperties = new Properties();
        // 设置kafka producer的属性
        // 详见http://kafka.apache.org/documentation/#configuration的Producer Configs
        producerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty("acks", "0"); // 0标识不等待brokers的确认， 1等待确认 all等待所有副本的确认
        producerProperties.setProperty("bootstrap.servers", "master:9092,slave1:9092");
        // producerProperties.setProperty("compression.type", "snappy");
        builder.setBolt("KafkaBolt", new KafkaBolt<String, String>()
                        .withTopicSelector("filtered-logs")
                        // K, V
                        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "filtered-logs"))
                        // 设置kafka producer的属性
                        .withProducerProperties(producerProperties)
                , 1).allGrouping("FilterBolt");
        builder.setBolt("StoredInFileBolt", new StoredInFileBolt(), 1).allGrouping("FilterBolt");
        builder.createTopology();

        Config config = new Config();
        if(args != null && args.length > 0){
            config.setDebug(false);
            StormSubmitter.submitTopology("flume-kafka-storm", config, builder.createTopology());
        }else{
            config.setDebug(true);
            LocalCluster cluster= new LocalCluster();
            cluster.submitTopology("flume-kafka-storm", config, builder.createTopology());
        }
    }
}
