package kafka.t2;


import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public TwitterProducer(){

    }
    Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String ConsumerKey = "S7FQu1gNM6gXai5rj7WgZt6WI";
    String ConsumerSecret = "SJ7CdgMIEJUqfgcvliCFkpssv98lsGCSSVy5TJa40eY34yTG3m";
    String token = "1045156166-CGrijn4VeijX35GtKBH9lV53NJ2191EPfC4uZfY";
    String secret = "GEli5vgr1egDfKOukHhd1a0uOOP9LHPmZOr92oo3QqKkX";

    public static void main(String[] args) {
        new TwitterProducer().run();
        //client twitter
    }

    public void run(){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);

        Client client = this.createTwitterCLient(msgQueue);

        client.connect();

        KafkaProducer<String, String > producer = this.createKafkaProducer();

        while (!client.isDone()) {

            String msg = null;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if(msg != null){
                producer.send(new ProducerRecord<>("twitter_msg", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            log.info("error", e);
                        }
                    }
                });

                log.info(msg);

            }

        }

        log.info("fin");

    }

    public Client createTwitterCLient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoins");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(ConsumerKey, ConsumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        return builder.build();

    }

    public KafkaProducer<String, String > createKafkaProducer(){
        String bootstrapServer = "localhost:9092";

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //KAFKA 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

        //high throughput producer (at the expense of a bit of latency anf CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //time to wait to batch size
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32kb batch size


        return new KafkaProducer<String, String>(properties);
    }
}
