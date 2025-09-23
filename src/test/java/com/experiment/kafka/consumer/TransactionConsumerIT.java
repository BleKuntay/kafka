package com.experiment.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {
        "transactions-idr",
        "transactions-usd",
        "transactions-dlt"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.ANY)
class TransactionConsumerIT {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testValidTransactionGoesToDb() throws Exception {
        // producer
        var producerProps = KafkaTestUtils.producerProps(broker);
        var template = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer())
        );

        Transaction tx = new Transaction("t1", "IDR", 5000);
        template.send("transactions-idr", mapper.writeValueAsString(tx));
        template.flush();

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM transactions WHERE id = 't1'", Integer.class);
            assertThat(count).isEqualTo(1);
        });
    }

    @Test
    void testInvalidTransactionGoesToDlt() throws Exception {
        var producerProps = KafkaTestUtils.producerProps(broker);
        var template = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer())
        );

        Transaction invalid = new Transaction("t2", "USD", -100); // invalid
        template.send("transactions-usd", mapper.writeValueAsString(invalid));
        template.flush();

        var consumerProps = KafkaTestUtils.consumerProps("test-group", "true", broker);
        consumerProps.put("auto.offset.reset", "earliest");
        var cf = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());
        var consumer = cf.createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, "transactions-dlt");

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(consumer, "transactions-dlt", Duration.ofSeconds(5));

        assertThat(record.value()).contains("\"id\":\"t2\"");
    }
}
