package com.experiment.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
public class TransactionConsumer {

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public static final String DLT_TOPIC = "transactions-dlt";

    @KafkaListener(topics = {"transactions-idr", "transactions-usd"}, groupId = "transaction-consumer")
    public void consume(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        executor.submit(() -> {
            try {
                Transaction tx = objectMapper.readValue(message, Transaction.class);

                if (tx.amount() <= 0) {
                    kafkaTemplate.send(DLT_TOPIC, message);
                } else {
                    jdbcTemplate.update(
                            "INSERT INTO transactions (id, currency, amount) VALUES (?, ?, ?)",
                            tx.id(), tx.currency(), tx.amount()
                    );
                }
            } catch (Exception e) {
                kafkaTemplate.send(DLT_TOPIC, message);
            }
        });
    }
}

