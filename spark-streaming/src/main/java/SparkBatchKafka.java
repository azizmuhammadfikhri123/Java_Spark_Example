import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.*;


public class SparkBatchKafka {
  public static void main(String[] args) {
    // Create SparkSession
    SparkSession spark = SparkSession
      .builder()
      .appName("KafkaToElasticsearch")
      .master("local[*]") // Set master URL here
      .config("spark.es.nodes", "192.168.20.90") // Elasticsearch nodes
      .config("spark.es.port", "9200") // Elasticsearch port
      .config("spark.es.mapping.id", "id")
      .config("spark.es.nodes.wan.only", "true") // Use only WAN IP addresses
      .getOrCreate();

    // Define JSON schema
    StructType schema = new StructType()
      .add("title", DataTypes.StringType)
      .add("content", DataTypes.createArrayType(DataTypes.StringType))
      .add("images", DataTypes.createArrayType(DataTypes.StringType))
      .add("crawling_at", DataTypes.StringType);

    // Read data from Kafka topic
    Dataset<Row> kafkaData = spark
      .read()
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") // Kafka broker address
      .option("subscribe", "forum_cia") // Kafka topic to subscribe to
      .load();

    // Convert Kafka data to DataFrame
    Dataset<Row> jsonData = kafkaData.selectExpr("CAST(value AS STRING) AS json")
      .select(functions.from_json(functions.col("json"), schema).as("data"));

    // Extract desired columns
    Dataset<Row> transformedData = jsonData.select("data.title", "data.content", "data.images", "data.crawling_at");
    transformedData.show(false);

    JavaRDD<Map<String, ?>> rddData = transformedData.javaRDD().map(row -> {
      Map<String, Object> map = new HashMap<>();
      map.put("id", UUID.randomUUID().toString());
      map.put("title", row.getString(0));
      map.put("content", row.getList(1));
      map.put("images", row.getList(2));
      map.put("crawling_at", row.getString(3));
      return map;
    });
    JavaEsSpark.saveToEs(rddData, "data_deepweb/_doc");

    // Stop SparkSession
    spark.stop();
  }
}
