  import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

public class SparkStreamKafka {

  public static <T> T getNestedValue(Map map, String... keys) {
    Object value = map;

    for (String key : keys) {
      value = ((Map) value).get(key);
    }

    return (T) value;
  }

  public static void main(String[] args) throws InterruptedException {
    ObjectMapper om = new ObjectMapper();

    // SPARK CONIFG IS HERE
    SparkConf sparkConf = new SparkConf()
      .setAppName("Spark Stream Kafka and insert to Elastic")
      .setMaster("local[2]")
      .set("es.index.auto.create","true")
      .set("es.nodes","192.168.20.90")
      .set("es.port", "9200")
      .set("es.mapping.id","id")
      .set("es.nodes.wan.only", "true")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.driver.only.wan","false");

    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(6000));

    // LOGGER CONFIG IS HERE
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.OFF);

    // KAFKA CONIFG IS HERE
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "localhost:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "kafkaConsumer2");
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", false);


    // TOPIC SET
    Collection<String> topics = Arrays.asList("forum_cia");

    // CREATE DIRECT STREAM FROM SPARK TO KAFKA
    JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
      LocationStrategies.PreferConsistent(),
      ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

    // CREATING TUPLE PAIR
    stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

    // GETTING DATA STREAM FROM KAFKA
    JavaDStream<String> lines = stream.map(record -> record.value());

    // Process each RDD per line
    lines.foreachRDD(rdd -> {
      JavaRDD<Map<String, Object>> rddData = rdd.distinct().map(x -> {
        Map<String, Object> finalMap = new HashMap<>();
        Map<String, Object> map = om.readValue(x.trim(), Map.class);

        finalMap.put("id", UUID.randomUUID().toString());
        finalMap.put("title", map.get("title"));
        finalMap.put("content", map.get("content"));
        finalMap.put("images", map.get("images"));
        finalMap.put("crawling_at", map.get("crawling_at"));
        return finalMap;
      }).filter(x -> !(x.isEmpty()));

      JavaEsSpark.saveToEs(rddData,"data_deepweb/_doc");
      rddData.foreach(x -> {
        System.out.println("==================================");
        System.out.println("Success for id: " + x.get("id"));
        System.out.println("===================================");
      });
    });

    jssc.start();
    jssc.awaitTermination();
  }
}
