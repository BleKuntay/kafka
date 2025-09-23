package com.experiment.kafka.streams.topology;

import com.experiment.kafka.streams.model.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test class for testing the Kafka Streams topology defined in the {@link TransactionStreamTopology} class.
 * This class validates the functionality of the topology by simulating input and output topics using a {@link TopologyTestDriver}.
 * The topology processes incoming transaction messages, routes them based on their currency, and writes
 * the results to the appropriate output topics.
 * <p>
 * Key functionalities tested:
 * - Setup and configuration of Kafka Streams topology using {@link StreamsBuilder}.
 * - Validation of the topology's ability to route transactions to specific output topics based on their currency.
 * - End-to-end testing of the transaction processing flow with simulated input and output.
 * <p>
 * Dependencies:
 * - {@link TopologyTestDriver}: Used to simulate the Kafka Streams environment for testing.
 * - {@link TestInputTopic}: Represents the input topic for feeding test data.
 * - {@link TestOutputTopic}: Represents the output topics for reading processed results.
 * - Jackson's {@link ObjectMapper}: Used for serializing and deserializing JSON data in test cases.
 * <p>
 * Test case:
 * - {@code shouldRouteTransactionBasedOnCurrency}: Simulates incoming transaction messages, applies the topology,
 *   and verifies the results are sent to the correct output topics based on the transaction's currency (IDR or USD).
 * <p>
 * Lifecycle methods:
 * - {@code setup}: Initializes the topology and test driver before each test case.
 * - {@code teardown}: Closes the test driver after each test case to release resources.
 */
class TransactionStreamTopologyTest {

    /**
     *
     */
    private TopologyTestDriver testDriver;
    /**
     * Represents the test input topic used in the {@link TransactionStreamTopologyTest} class
     * to simulate input data for Kafka Streams during unit testing. This topic is configured
     **/
    private TestInputTopic<String, String> inputTopic;
    /**
     * TestOutputTopic used to capture and assert the contents of the Kafka output topic
     * for transactions in "IDR" currency within the test scenario.
     * <p>
     * This variable serves as a mocked representation of the OUTPUT_TOPIC_IDR defined
     * in the {@link TransactionStreamTopology} class, allowing the verification*/
    private TestOutputTopic<String, String> outputIdr;
    /**
     *
     */
    private TestOutputTopic<String, String> outputUsd;
    /**
     * A thread-safe and reusable instance of {@link ObjectMapper} used for JSON
     * serialization and deserialization in test cases. This object is utilized for
     * converting between Java objects and JSON strings during test execution.
     */
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Sets up the testing environment for Kafka Streams topology processing in the context
     * of the `TransactionStreamTopologyTest`. This method is executed before each test case
     * to initialize the required components, including the Kafka Streams topology, test driver,
     * input and output topics.
     * <p>
     * Responsibilities:
     * - Constructs a new `StreamsBuilder` to define the Kafka Streams topology.
     * - Creates a virtual thread-based executor service for asynchronous task execution.
     * - Instantiates the `TransactionStreamTopology` and configures the Kafka Streams topology using the provided builder and executor.
     * - Sets up*/
    @BeforeEach
    void setup() {
        var builder = new StreamsBuilder();
        var executor = Executors.newVirtualThreadPerTaskExecutor();

        new TransactionStreamTopology().transactionTopology(builder, executor);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic(
                TransactionStreamTopology.INPUT_TOPIC,
                Serdes.String().serializer(),
                Serdes.String().serializer()
        );
        outputIdr = testDriver.createOutputTopic(
                TransactionStreamTopology.OUTPUT_TOPIC_IDR,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        );
        outputUsd = testDriver.createOutputTopic(
                TransactionStreamTopology.OUTPUT_TOPIC_USD,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        );
    }

    /**
     * Cleans up resources or components used during the test execution. This method
     * is executed automatically after each test case to ensure that the test environment
     * is properly reset and any external resources are released.
     * <p>
     * Responsibilities:
     * - Closes the test driver instance to release resources associated with it.
     */
    @AfterEach
    void teardown() {
        testDriver.close();
    }


    /**
     * Tests the routing logic of the Kafka Streams topology based on the transaction currency.
     * This test ensures that transactions with different currencies are routed to their respective
     * output topics according to the defined Kafka Streams topology.
     *
     * The test scenario involves:
     * - Creating two mock {@link Transaction} objects with different currency values ("IDR" and "USD").
     * - Piping these transactions into the input topic of the Kafka Streams topology.
     * - Verifying that the transactions are routed to the correct output topics:
     *   - Transactions with "IDR" currency should appear in the `outputIdr` topic.
     *   - Transactions with "USD" currency should appear in the `outputUsd` topic.
     *
     * Assertions:
     * - Confirms that the output records for "IDR" transactions contain the correct currency identifier.
     * - Confirms that the output records for "USD" transactions contain the correct currency identifier.
     *
     * @throws Exception if any error occurs during the test execution
     */
    @Test
    void shouldRouteTransactionBasedOnCurrency() throws Exception {
        var tx1 = new Transaction("1", "IDR", 1000);
        var tx2 = new Transaction("2", "USD", 200);

        inputTopic.pipeInput("key1", mapper.writeValueAsString(tx1));
        inputTopic.pipeInput("key2", mapper.writeValueAsString(tx2));

        var idrResult = outputIdr.readRecord();
        var usdResult = outputUsd.readRecord();

        assertThat(idrResult.value()).contains("\"currency\":\"IDR\"");
        assertThat(usdResult.value()).contains("\"currency\":\"USD\"");
    }
}
