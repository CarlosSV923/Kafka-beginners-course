package kafka.t1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServer = "localhost:9092";

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++){

            String topic = "primerTopic";

            String value = "Hello World " + Integer.toString(i);

            String key = "Key_" + i;

            ProducerRecord<String, String> record = new ProducerRecord(topic, key, value);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        log.info(recordMetadata.topic());
                    }else{
                        log.error("fallo productor", e);
                    }
                }
            });


        }

        producer.flush();

        producer.close();

    }
}
