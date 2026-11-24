package dev.ivanov.order_service;

public record Order(
        String orderId,
        String product,
        Integer quantity
) { }

