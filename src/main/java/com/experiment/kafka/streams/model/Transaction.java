package com.experiment.kafka.streams.model;

public record Transaction(String id, String currency, double amount) {
}