package com.example;

public interface QueueService {
    /** push a message onto a queue. */
    public void push(String queueUrl, String messageBody);

    /** push a message with priority onto a queue. Higher priority values are processed first. */
    public void push(String queueUrl, String messageBody, int priority);

    /** retrieves a single message from a queue. */
    public Message pull(String queueUrl);

    /** deletes a message from the queue that was received by pull(). */
    public void delete(String queueUrl, String receiptId);
}
