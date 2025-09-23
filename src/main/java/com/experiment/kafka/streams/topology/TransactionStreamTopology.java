package com.experiment.kafka.streams.topology;

import com.experiment.kafka.streams.model.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TransactionStreamTopology is a Spring configuration class that defines the Kafka Streams
 * topology for processing transaction messages. The topology processes incoming messages,
 * deserializes them into Transaction objects, and routes them to specific Kafka topics
 * based on the currency type. It uses a virtual thread executor for concurrent processing.
 * <p>
 * The class defines the following topics:
 * - INPUT_TOPIC: The input topic for incoming transaction messages.
 * - OUTPUT_TOPIC_IDR: The output topic to which transactions with "IDR" currency are routed.
 * - OUTPUT_TOPIC_USD: The output topic to which transactions with "USD" currency are routed.
 * <p>
 * Beans provided by this class:
 * - {@link ExecutorService}: Responsible for executing tasks using virtual threads.
 * - {@link Topology}: Defines the Kafka Streams topology for transaction processing.
 * <p>
 * Functionality:
 * - Reads messages from the input topic.
 * - Logs received messages using the provided executor.
 * - Deserializes messages into Transaction objects using Jackson's ObjectMapper.
 * - Splits the stream based on the transaction currency.
 *   - Routes "IDR" transactions to the OUTPUT_TOPIC_IDR.
 *   - Routes "USD" transactions to the OUTPUT_TOPIC_USD.
 * <p>
 * Exceptions during message processing (e.g., deserialization errors) are logged as runtime exceptions.
 */
@Configuration
public class TransactionStreamTopology {

    /**
     * A thread-safe and reusable instance of {@link ObjectMapper} used for serializing
     * and deserializing objects to and from JSON within the {@code TransactionStreamTopology} class.
     * This instance is configured for general-purpose JSON processing.
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * The name of the Kafka topic from which transaction data will be consumed.
     */
    public static final String INPUT_TOPIC = "transactions-input";
    /**
     * The name of the Kafka topic where transaction records in IDR currency
     * are published. This constant is used as the output topic for processing
     * and streaming transactions related to the IDR (Indonesian Rupiah) currency
     * in the system's topology.
     */
    public static final String OUTPUT_TOPIC_IDR = "transactions-idr";
    /**
     * The name of the Kafka topic where transactions with USD currency are published.
     * Used in the topology to route and store transaction data in USD for further processing.
     */
    public static final String OUTPUT_TOPIC_USD = "transactions-usd";

    /**
     * Creates and returns an {@link ExecutorService} configured to use a virtual thread for each task.
     * A virtual thread is a lightweight thread provided by the Java Virtual Machine (JVM),
     * which enables high-concurrency operations in a more efficient way compared to platform threads.
     * The returned executor service is intended to simplify running asynchronous tasks by leveraging virtual threads.
     * <p>
     * The lifecycle of the executor is tied to the Spring container. When the container shuts down,
     * the {@code close} method will be invoked to properly release resources.
     *
     * @return an {@link ExecutorService} implementation that creates a new virtual thread per task.
     */
    @Bean(destroyMethod = "close")
    public ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Builds and returns a Kafka Streams {@link Topology} that processes transaction data.
     * The method consumes transaction records from a predefined input topic, filters
     * and processes the transactions based on their currency, and then routes them to
     * appropriate output topics.
     *
     * @param builder the {@link StreamsBuilder} used to define the Kafka Streams topology
     * @param executor the {@link ExecutorService} used to handle asynchronous tasks such as logging
     * @return the constructed {@link Topology} for processing transactions
     */
    @Bean
    public Topology transactionTopology(StreamsBuilder builder, ExecutorService executor) {
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> executor.execute(() -> System.out.println("Received: " + value)))
                .mapValues(value -> {
                    try {
                        return objectMapper.readValue(value, Transaction.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .split()
                .branch((key, tx) -> "IDR".equalsIgnoreCase(tx.currency()), Branched.withConsumer(ks ->
                        ks.mapValues(this::toJson)
                                .to(OUTPUT_TOPIC_IDR, Produced.with(Serdes.String(), Serdes.String()))))
                .branch((key, tx) -> "USD".equalsIgnoreCase(tx.currency()), Branched.withConsumer(ks ->
                        ks.mapValues(this::toJson)
                                .to(OUTPUT_TOPIC_USD, Produced.with(Serdes.String(), Serdes.String()))))
                .noDefaultBranch();

        return builder.build();
    }

    private String toJson(Transaction tx) {
        try {
            return objectMapper.writeValueAsString(tx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
