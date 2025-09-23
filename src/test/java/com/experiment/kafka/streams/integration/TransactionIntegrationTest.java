package com.experiment.kafka.streams.integration;

import com.experiment.kafka.streams.model.Transaction;
import com.experiment.kafka.streams.topology.TransactionStreamTopology;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for processing and verifying the Kafka message flow in the transaction stream.
 * This test validates the end-to-end behavior of the Kafka Streams topology defined in
 * {@link TransactionStreamTopology}.
 * <p>
 * An embedded Kafka instance is used to produce and consume messages for testing. The test:
 * - Publishes a sample transaction message to the input topic.
 * - Reads the output from the corresponding output topic.
 * - Verifies that the message is processed and routed correctly based on the transaction currency.
 * <p>
 * An {@link EmbeddedKafkaBroker} is utilized for configuring and controlling the embedded Kafka instance.
 * <p>
 * An {@link ObjectMapper} is used for serialization and deserialization of transaction objects
 * to and from JSON format.
 * <p>
 * Test methods in this class perform the following steps:
 * - Set up Kafka producer properties and send a transaction message to the input topic.
 * - Configure Kafka consumer properties and consume messages from the expected output topic.
 * - Assert the correctness of the processed output with the expected result.
 * <p>
 * Special attention is placed on ensuring messages with "IDR" currency are directed to the
 * appropriate topic.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {
        TransactionStreamTopology.INPUT_TOPIC,
        TransactionStreamTopology.OUTPUT_TOPIC_IDR,
        TransactionStreamTopology.OUTPUT_TOPIC_USD
})
class TransactionIntegrationTest {

    /**
     * The {@code broker} refers to an instance of {@link EmbeddedKafkaBroker} that is utilized
     * for integration testing with an embedded Kafka setup.
     * <p>
     * This broker provides the necessary infrastructure for testing Kafka-based message flows
     * without requiring a separate Kafka cluster. It facilitates producing and consuming messages
     * on predefined topics for validation purposes.
     * <p>
     * It is primarily used in the {@code TransactionIntegrationTest} class to:
     * - Simulate Kafka topics for message processing.
     * - Provide topic handling, such as consuming messages and reading offsets.
     * - Set up producer and consumer properties using {@link KafkaTestUtils}.
     * <p>
     * Spring's {@link Autowired} annotation is used to inject this bean during test initialization.
     */
    @Autowired
    private EmbeddedKafkaBroker broker;

    /**
     * A thread-safe and reusable instance of {@link ObjectMapper} used for serializing and deserializing
     * objects to and from JSON within the integration test. The {@code mapper} is utilized for
     * converting {@link Transaction} objects to their JSON representation during Kafka message production,
     * as well as parsing JSON strings back into objects when verifying Kafka message processing.
     * This instance is configured for general-purpose JSON processing.
     */
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Tests the Kafka message flow for transactions using the embedded Kafka environment.
     * This test verifies the end-to-end processing of a transaction message through
     * the Kafka Streams topology defined in {@link TransactionStreamTopology}.
     * <p>
     * Steps performed by this test include:
     * 1. Configuring a Kafka producer to send a sample transaction message to the input topic.
     * 2. Sending a transaction with "IDR" currency to the input topic.
     * 3. Configuring a Kafka consumer to retrieve messages from the output topic for "IDR" transactions.
     * 4. Verifying that the transaction message is correctly routed to the output topic and matches
     *    expected content, including the "IDR" currency in the serialized message payload.
     * <p>
     * Assertions:
     * - The message retrieved from the output topic must contain the currency field with a value of "IDR".
     *
     * @throws Exception if an error occurs during the execution of the test, such as serialization,
     *                   message routing or Kafka operations.
     */
    @Test
    void testMessageFlow() throws Exception {
        var producerProps = KafkaTestUtils.producerProps(broker);
        var pf = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer());
        var template = new KafkaTemplate<>(pf);

        var tx = new Transaction("10", "IDR", 5000);
        template.send(TransactionStreamTopology.INPUT_TOPIC, "k1", mapper.writeValueAsString(tx));
        template.flush();

        var consumerProps = KafkaTestUtils.consumerProps("group1", "true", broker);
        consumerProps.put("auto.offset.reset", "earliest");
        var cf = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());
        var consumer = cf.createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, TransactionStreamTopology.OUTPUT_TOPIC_IDR);

        var record = KafkaTestUtils.getSingleRecord(
                consumer,
                TransactionStreamTopology.OUTPUT_TOPIC_IDR,
                Duration.ofSeconds(10)
        );

        System.out.println("RECORD: " + record.value());

        assertThat(record.value()).contains("\"currency\":\"IDR\"");
    }

}
