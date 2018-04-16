package co.edu.uniandes.mati.nuevageneracion.taller3;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

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
    private static final Pattern SPACE = Pattern.compile(" ");

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
        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("GenericRealTimeProcessor").setMaster("spark://" + sparkMasterNode);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

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


        JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>) x -> Lists.newArrayList(SPACE.split(x)));


        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)).reduceByKey(
                (Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        wordCounts.print();

        //
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
