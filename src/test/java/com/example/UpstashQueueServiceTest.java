package com.example;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for UpstashQueueService.
 * These tests require valid Upstash Redis credentials in environment variables.
 * Tests are only enabled when UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN are set.
 */
@EnabledIfEnvironmentVariable(named = "UPSTASH_REDIS_REST_URL", matches = ".*")
@EnabledIfEnvironmentVariable(named = "UPSTASH_REDIS_REST_TOKEN", matches = ".*")
class UpstashQueueServiceTest {

    private UpstashQueueService queueService;
    private static final String TEST_QUEUE = "test-queue-" + System.currentTimeMillis();

    @BeforeEach
    void setUp() {
        try {
            queueService = new UpstashQueueService();
        } catch (IllegalArgumentException e) {
            // Skip tests if credentials are not available
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "Upstash credentials not available");
        }
    }

    @Test
    @DisplayName("Should push and pull messages with default priority")
    void testBasicPushPull() {
        queueService.push(TEST_QUEUE, "test message");
        
        Message message = queueService.pull(TEST_QUEUE);
        
        assertNotNull(message);
        assertEquals("test message", message.getBody());
        assertNotNull(message.getReceiptId());
        
        // Clean up
        queueService.delete(TEST_QUEUE, message.getReceiptId());
    }

    @Test
    @DisplayName("Should handle priority ordering correctly")
    void testPriorityOrdering() {
        // Push messages with different priorities
        queueService.push(TEST_QUEUE + "-priority", "low priority", 1);
        queueService.push(TEST_QUEUE + "-priority", "high priority", 10);
        queueService.push(TEST_QUEUE + "-priority", "medium priority", 5);
        
        // Pull messages and verify they come out in priority order
        Message first = queueService.pull(TEST_QUEUE + "-priority");
        assertEquals("high priority", first.getBody());
        
        Message second = queueService.pull(TEST_QUEUE + "-priority");
        assertEquals("medium priority", second.getBody());
        
        Message third = queueService.pull(TEST_QUEUE + "-priority");
        assertEquals("low priority", third.getBody());
        
        // Clean up
        queueService.delete(TEST_QUEUE + "-priority", first.getReceiptId());
        queueService.delete(TEST_QUEUE + "-priority", second.getReceiptId());
        queueService.delete(TEST_QUEUE + "-priority", third.getReceiptId());
    }

    @Test
    @DisplayName("Should maintain FIFO order within same priority")
    void testFIFOWithinSamePriority() throws InterruptedException {
        String queueName = TEST_QUEUE + "-fifo";
        
        // Push multiple messages with same priority
        queueService.push(queueName, "first", 5);
        Thread.sleep(10); // Ensure different timestamps
        queueService.push(queueName, "second", 5);
        Thread.sleep(10);
        queueService.push(queueName, "third", 5);
        
        // Should come out in FIFO order
        Message first = queueService.pull(queueName);
        Message second = queueService.pull(queueName);
        Message third = queueService.pull(queueName);
        
        assertEquals("first", first.getBody());
        assertEquals("second", second.getBody());
        assertEquals("third", third.getBody());
        
        // Clean up
        queueService.delete(queueName, first.getReceiptId());
        queueService.delete(queueName, second.getReceiptId());
        queueService.delete(queueName, third.getReceiptId());
    }

    @Test
    @DisplayName("Should handle mixed priority and non-priority messages")
    void testMixedPriorityMessages() {
        String queueName = TEST_QUEUE + "-mixed";
        
        // Push non-priority message (default priority 0)
        queueService.push(queueName, "default priority");
        
        // Push high priority message
        queueService.push(queueName, "high priority", 10);
        
        // High priority should come first
        Message first = queueService.pull(queueName);
        Message second = queueService.pull(queueName);
        
        assertEquals("high priority", first.getBody());
        assertEquals("default priority", second.getBody());
        
        // Clean up
        queueService.delete(queueName, first.getReceiptId());
        queueService.delete(queueName, second.getReceiptId());
    }

    @Test
    @DisplayName("Should return null when queue is empty")
    void testEmptyQueue() {
        String emptyQueue = TEST_QUEUE + "-empty";
        Message message = queueService.pull(emptyQueue);
        assertNull(message);
    }

    @Test
    @DisplayName("Should handle message deletion")
    void testMessageDeletion() {
        String queueName = TEST_QUEUE + "-delete";
        
        queueService.push(queueName, "test message");
        
        Message message = queueService.pull(queueName);
        assertNotNull(message);
        
        // Delete the message
        queueService.delete(queueName, message.getReceiptId());
        
        // Queue should be empty
        assertNull(queueService.pull(queueName));
    }

    @Test
    @DisplayName("Should handle negative priorities")
    void testNegativePriorities() {
        String queueName = TEST_QUEUE + "-negative";
        
        queueService.push(queueName, "negative priority", -5);
        queueService.push(queueName, "zero priority", 0);
        queueService.push(queueName, "positive priority", 3);
        
        // Should come out in priority order (highest first)
        Message first = queueService.pull(queueName);
        Message second = queueService.pull(queueName);
        Message third = queueService.pull(queueName);
        
        assertEquals("positive priority", first.getBody());
        assertEquals("zero priority", second.getBody());
        assertEquals("negative priority", third.getBody());
        
        // Clean up
        queueService.delete(queueName, first.getReceiptId());
        queueService.delete(queueName, second.getReceiptId());
        queueService.delete(queueName, third.getReceiptId());
    }

    @Test
    @DisplayName("Should handle multiple queues independently")
    void testMultipleQueues() {
        String queue1 = TEST_QUEUE + "-multi1";
        String queue2 = TEST_QUEUE + "-multi2";
        
        queueService.push(queue1, "message1", 5);
        queueService.push(queue2, "message2", 10);
        
        Message msg1 = queueService.pull(queue1);
        Message msg2 = queueService.pull(queue2);
        
        assertEquals("message1", msg1.getBody());
        assertEquals("message2", msg2.getBody());
        
        // Each queue should be independent
        assertNull(queueService.pull(queue1));
        assertNull(queueService.pull(queue2));
        
        // Clean up
        queueService.delete(queue1, msg1.getReceiptId());
        queueService.delete(queue2, msg2.getReceiptId());
    }

    @Test
    @DisplayName("Should handle concurrent operations")
    void testConcurrentOperations() throws InterruptedException {
        String queueName = TEST_QUEUE + "-concurrent";
        int numberOfMessages = 20;
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(2);
        ConcurrentLinkedQueue<Message> pulledMessages = new ConcurrentLinkedQueue<>();

        // Producer thread
        executor.submit(() -> {
            try {
                for (int i = 0; i < numberOfMessages; i++) {
                    queueService.push(queueName, "message-" + i, i % 5);
                    Thread.sleep(10); // Small delay to ensure different timestamps
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        });

        // Consumer thread
        executor.submit(() -> {
            try {
                Thread.sleep(100); // Let producer add some messages first
                for (int i = 0; i < numberOfMessages; i++) {
                    Message msg = queueService.pull(queueName);
                    if (msg != null) {
                        pulledMessages.add(msg);
                        queueService.delete(queueName, msg.getReceiptId());
                    }
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        });

        assertTrue(latch.await(60, TimeUnit.SECONDS));
        executor.shutdown();

        // Verify messages were processed
        assertTrue(pulledMessages.size() > 0);
        
        // Verify priority ordering in pulled messages
        List<Message> sortedMessages = new ArrayList<>(pulledMessages);
        for (int i = 0; i < sortedMessages.size() - 1; i++) {
            assertTrue(sortedMessages.get(i).getPriority() >= sortedMessages.get(i + 1).getPriority(),
                "Messages should be ordered by priority");
        }
    }
}
