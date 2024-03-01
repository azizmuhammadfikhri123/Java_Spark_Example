import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;

public class SparkBatchCSV {
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

    // Define JSON schema
    StructType schema = new StructType()
      .add("koridor", DataTypes.IntegerType)
      .add("current_trip_id", DataTypes.StringType)
      .add("color", DataTypes.StringType)
      .add("gps_datetime", DataTypes.StringType)
      .add("dtd", DataTypes.DoubleType)
      .add("trip_name", DataTypes.StringType)
      .add("course", DataTypes.IntegerType)
      .add("location", DataTypes.StringType)
      .add("bus_code", DataTypes.StringType)
      .add("speed", DataTypes.DoubleType)
      .add("timestamp", DataTypes.StringType)
      .add("geo_location", DataTypes.StringType);

    Dataset<Row> CSVData = spark
      .read()
      .option("headers", "true")
      .option("delimiter", ";")
      .schema(schema)
      .csv("/home/be-azizmuhammadf/Documents/BELAJAR/SPRING BOOT/spark-streaming/src/main/java/data_sample/combine.csv");

    Dataset<Row> DataWithoutHeader = CSVData.except(CSVData.limit(1));
    Dataset<Row> updatedData = DataWithoutHeader.withColumn("geo_location", functions.udf(
      (String geoLocation) -> {
          Double[] geoArray = om.readValue(geoLocation, Double[].class);
          return Arrays.asList(geoArray);

      }, DataTypes.createArrayType(DataTypes.DoubleType)).apply(functions.col("geo_location")));

    updatedData.printSchema();
    updatedData.show(false);

    JavaRDD<Map<String, ?>> rddData = updatedData.javaRDD().map(row -> {
      Map<String, Object> finalMap = new HashMap<>();
      List<Double> geoLocationList = JavaConverters.seqAsJavaList((Seq<Double>) row.get(11));

      finalMap.put("id", UUID.randomUUID().toString());
      finalMap.put("koridor", row.get(0));
      finalMap.put("current_trip_id", row.get(1));
      finalMap.put("color", row.get(2));
      finalMap.put("gps_datetime", row.get(3));
      finalMap.put("dtd", row.get(4));
      finalMap.put("trip_name", row.get(5));
      finalMap.put("course", row.get(6));
      finalMap.put("location", row.get(7));
      finalMap.put("bus_code", row.get(8));
      finalMap.put("speed", row.get(9));
      finalMap.put("timestamp", row.get(10));
      finalMap.put("geo_location", geoLocationList);
      return finalMap;
    });
    JavaEsSpark.saveToEs(rddData, "transjakarta/_doc");

    // Stop SparkSession
    spark.stop();
  }
}
