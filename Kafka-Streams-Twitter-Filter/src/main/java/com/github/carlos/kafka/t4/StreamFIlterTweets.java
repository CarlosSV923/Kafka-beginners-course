package com.github.carlos.kafka.t4;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import com.google.gson.JsonParser;

import java.util.Properties;

public class StreamFIlterTweets {
    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_topic");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweets) -> extracUsersFollowersInTweet(jsonTweets) > 10000
        );

        filteredStream.to("important_tweets");

        //build topology

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        //start our streams application

        kafkaStreams.start();

    }

    public static int extracUsersFollowersInTweet(String jsonString){
        try{
            return jsonParser.parse(jsonString).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();

        }catch (NullPointerException e){
            return 0;
        }
    }
}
