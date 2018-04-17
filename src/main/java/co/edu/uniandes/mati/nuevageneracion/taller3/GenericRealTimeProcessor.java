package co.edu.uniandes.mati.nuevageneracion.taller3;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.Document;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.eq;

/**
 * Consumes messages from one or more topics in Kafka and does an insert in a non relational data base called Mongo.
 * Usage: GenericRealTimeProcessor <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.GenericRealTimeProcessor broker1-host:port,broker2-host:port topic1,topic2
 *
 * @author Alex Vicente Chacon Jimenez (alex.chacon@gmail.com)
 * @author Gabriel Zapata (gab.j.zapata@gmail.com)
 * @author Andres Sarmiento (ansarmientoto@gmail.com)
 *
 * @since JDK 1.8
 */
public class GenericRealTimeProcessor
{
    public static void main(String[] args)
    {
        if (args.length < 2)
        {
            System.err.println("Usage: GenericRealTimeProcessor <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];
        String sparkMasterNode= args[2];

        //
        // Mongo connection parameters
        String mongoServerIp= args[3];
        String mongoServerPort= args[4];
        String database= args[5];
        String topicCollection= args[6];

        //
        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("GenericRealTimeProcessor").setMaster("spark://" + sparkMasterNode);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(500));

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        //
        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map((Function<Tuple2<String, String>, String>) Tuple2::_2);

        lines.foreachRDD((Function<JavaRDD<String>, Void>) rdd -> {
            rdd.foreach((VoidFunction<String>) s -> {
                Logger.getAnonymousLogger().info(s);
                mongoConnection(Integer.parseInt(s),  mongoServerIp, mongoServerPort, database, topicCollection);
            });
            return null;
        });

        //
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    private static void mongoConnection (Integer sensorValue, String mongoServerIp, String mongoServerPort, String databaseName, String topicCollection)
    {
        MongoClient mongoClient = new MongoClient(mongoServerIp, Integer.parseInt(mongoServerPort));
        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(topicCollection);

        if (collection.count() > 100 )
        {
            collection.findOneAndDelete(eq("id", "9"));
        }

        Document  document = new Document ("id", "9")
                .append("value", sensorValue)
                .append("timestamp", System.currentTimeMillis());
        collection.insertOne(document);

    }
}
