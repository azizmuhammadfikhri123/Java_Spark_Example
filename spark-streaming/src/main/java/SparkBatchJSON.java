import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SparkBatchJSON {
  public static void main(String[] args) {
    ObjectMapper om = new ObjectMapper();

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

    // read file JSON
    Dataset<Row> news = spark.read()
      .json("/home/be-azizmuhammadf/Documents/BELAJAR/SPRING BOOT/spark-streaming/src/main/java/data_sample/news.json");
    
    news.createOrReplaceTempView("twitter_data");

    // SELECT DATA
    Dataset<Row> user_info = spark.sql("SELECT user.id , user.username, user.location, user.favouritesCount, user.friendsCount, user.followersCount, user.mediaCount, user.favouritesCount, user.listedCount, user.mediaCount, user.verified, content, date FROM twitter_data WHERE user IS NOT NULL");
    user_info.printSchema();
    user_info.show(false);

    JavaRDD<Row> data_rdd = user_info.javaRDD();
    JavaRDD<Map<String, Object>> userMapRDD = data_rdd.map(row -> {
      Map<String, Object> finalMap = new HashMap<>();
      String jsonString = row.json();
      Map<String, Object> map = om.readValue(jsonString, Map.class);
      SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+00:00'");

      Date d = df1.parse(map.get("date").toString());
      long timeInMillis = d.getTime();
      finalMap.put("id", map.get("id"));
      finalMap.put("created_at",timeInMillis);
      finalMap.put("username", map.get("username"));
      finalMap.put("location", map.get("location") == null || map.get("location") == "" ? "" : map.get("location"));
      finalMap.put("favouritesCount", map.get("favouritesCount"));
      finalMap.put("friendsCount", map.get("friendsCount"));
      finalMap.put("followersCount", map.get("followersCount"));
      finalMap.put("mediaCount", map.get("mediaCount"));
      finalMap.put("verified", map.get("verified"));
      finalMap.put("description", map.get("content").toString().trim());

      return finalMap;
    });

    JavaEsSpark.saveToEs(userMapRDD, "news_tweet/_doc");
  }
}
