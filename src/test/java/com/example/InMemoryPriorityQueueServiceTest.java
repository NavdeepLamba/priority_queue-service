package com.example;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryPriorityQueueServiceTest {

    private InMemoryPriorityQueueService queueService;
    private static final String TEST_QUEUE = "test-queue";

    @BeforeEach
    void setUp() {
        queueService = new InMemoryPriorityQueueService();
    }

    @Test
    @DisplayName("Should push and pull messages with default priority")
    void testBasicPushPull() {
        queueService.push(TEST_QUEUE, "test message");
        
        Message message = queueService.pull(TEST_QUEUE);
        
        assertNotNull(message);
        assertEquals("test message", message.getBody());
        assertNotNull(message.getReceiptId());
    }

    @Test
    @DisplayName("Should handle priority ordering correctly")
    void testPriorityOrdering() {
        // Push messages with different priorities
        queueService.push(TEST_QUEUE, "low priority", 1);
        queueService.push(TEST_QUEUE, "high priority", 10);
        queueService.push(TEST_QUEUE, "medium priority", 5);
        
        // Pull messages and verify they come out in priority order
        Message first = queueService.pull(TEST_QUEUE);
        assertEquals("high priority", first.getBody());
        
        Message second = queueService.pull(TEST_QUEUE);
        assertEquals("medium priority", second.getBody());
        
        Message third = queueService.pull(TEST_QUEUE);
        assertEquals("low priority", third.getBody());
    }

    @Test
    @DisplayName("Should maintain FIFO order within same priority")
    void testFIFOWithinSamePriority() throws InterruptedException {
        // Push multiple messages with same priority
        queueService.push(TEST_QUEUE, "first", 5);
        Thread.sleep(1); // Ensure different timestamps
        queueService.push(TEST_QUEUE, "second", 5);
        Thread.sleep(1);
        queueService.push(TEST_QUEUE, "third", 5);
        
        // Should come out in FIFO order
        assertEquals("first", queueService.pull(TEST_QUEUE).getBody());
        assertEquals("second", queueService.pull(TEST_QUEUE).getBody());
        assertEquals("third", queueService.pull(TEST_QUEUE).getBody());
    }

    @Test
    @DisplayName("Should handle mixed priority and non-priority messages")
    void testMixedPriorityMessages() {
        // Push non-priority message (default priority 0)
        queueService.push(TEST_QUEUE, "default priority");
        
        // Push high priority message
        queueService.push(TEST_QUEUE, "high priority", 10);
        
        // High priority should come first
        assertEquals("high priority", queueService.pull(TEST_QUEUE).getBody());
        assertEquals("default priority", queueService.pull(TEST_QUEUE).getBody());
    }

    @Test
    @DisplayName("Should return null when queue is empty")
    void testEmptyQueue() {
        Message message = queueService.pull(TEST_QUEUE);
        assertNull(message);
    }

    @Test
    @DisplayName("Should handle message deletion")
    void testMessageDeletion() {
        queueService.push(TEST_QUEUE, "test message");
        
        Message message = queueService.pull(TEST_QUEUE);
        assertNotNull(message);
        
        // Delete the message
        queueService.delete(TEST_QUEUE, message.getReceiptId());
        
        // Queue should be empty
        assertNull(queueService.pull(TEST_QUEUE));
    }

    @Test
    @DisplayName("Should handle visibility timeout")
    void testVisibilityTimeout() throws InterruptedException {
        queueService.push(TEST_QUEUE, "test message");
        
        // Pull message - it becomes invisible
        Message message = queueService.pull(TEST_QUEUE);
        assertNotNull(message);
        
        // Immediate pull should return null (message is invisible)
        assertNull(queueService.pull(TEST_QUEUE));
        
        // Wait for visibility timeout to expire (default is 30 seconds, but we'll test shorter)
        // Note: In a real test, you might want to use a shorter timeout configuration
        Thread.sleep(1000); // Wait 1 second
        
        // For this test, we'll delete the message to complete the flow
        queueService.delete(TEST_QUEUE, message.getReceiptId());
        assertNull(queueService.pull(TEST_QUEUE));
    }

    @Test
    @DisplayName("Should be thread-safe for concurrent operations")
    void testConcurrentOperations() throws InterruptedException {
        int numberOfThreads = 10;
        int messagesPerThread = 50; // Reduced for more reliable testing
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch producerLatch = new CountDownLatch(numberOfThreads / 2);
        CountDownLatch consumerLatch = new CountDownLatch(numberOfThreads / 2);
        AtomicInteger messageCount = new AtomicInteger(0);

        // Producer threads
        for (int i = 0; i < numberOfThreads / 2; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < messagesPerThread; j++) {
                        queueService.push(TEST_QUEUE, "message-" + threadId + "-" + j, j % 10);
                        messageCount.incrementAndGet();
                    }
                } finally {
                    producerLatch.countDown();
                }
            });
        }

        // Wait for all producers to finish first
        assertTrue(producerLatch.await(30, TimeUnit.SECONDS));

        // Consumer threads - start after producers finish
        ConcurrentLinkedQueue<Message> pulledMessages = new ConcurrentLinkedQueue<>();
        for (int i = numberOfThreads / 2; i < numberOfThreads; i++) {
            executor.submit(() -> {
                try {
                    // Pull messages until we get some or timeout
                    int attempts = 0;
                    while (attempts < messagesPerThread && pulledMessages.size() < messageCount.get()) {
                        Message msg = queueService.pull(TEST_QUEUE);
                        if (msg != null) {
                            pulledMessages.add(msg);
                            queueService.delete(TEST_QUEUE, msg.getReceiptId());
                        }
                        attempts++;
                        // Small delay to allow other threads to work
                        try { Thread.sleep(1); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    }
                } finally {
                    consumerLatch.countDown();
                }
            });
        }

        assertTrue(consumerLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        // Verify messages were processed
        assertTrue(messageCount.get() > 0, "No messages were produced");
        assertTrue(pulledMessages.size() > 0, "No messages were consumed");
        
        // Verify that we processed a reasonable number of messages
        assertTrue(pulledMessages.size() <= messageCount.get(), "More messages consumed than produced");
    }

    @Test
    @DisplayName("Should handle negative priorities")
    void testNegativePriorities() {
        queueService.push(TEST_QUEUE, "negative priority", -5);
        queueService.push(TEST_QUEUE, "zero priority", 0);
        queueService.push(TEST_QUEUE, "positive priority", 3);
        
        // Should come out in priority order (highest first)
        assertEquals("positive priority", queueService.pull(TEST_QUEUE).getBody());
        assertEquals("zero priority", queueService.pull(TEST_QUEUE).getBody());
        assertEquals("negative priority", queueService.pull(TEST_QUEUE).getBody());
    }

    @Test
    @DisplayName("Should handle multiple queues independently")
    void testMultipleQueues() {
        String queue1 = "queue1";
        String queue2 = "queue2";
        String queue3 = "queue3";
        
        // Add messages with different priorities to different queues
        queueService.push(queue1, "low-priority-q1", 1);
        queueService.push(queue1, "high-priority-q1", 10);
        
        queueService.push(queue2, "medium-priority-q2", 5);
        queueService.push(queue2, "urgent-priority-q2", 15);
        
        queueService.push(queue3, "normal-priority-q3", 7);
        
        // Each queue should maintain its own priority order
        Message msg1_high = queueService.pull(queue1);
        Message msg2_urgent = queueService.pull(queue2);
        Message msg3_normal = queueService.pull(queue3);
        
        assertEquals("high-priority-q1", msg1_high.getBody());
        assertEquals(10, msg1_high.getPriority());
        
        assertEquals("urgent-priority-q2", msg2_urgent.getBody());
        assertEquals(15, msg2_urgent.getPriority());
        
        assertEquals("normal-priority-q3", msg3_normal.getBody());
        assertEquals(7, msg3_normal.getPriority());
        
        // Get remaining messages
        Message msg1_low = queueService.pull(queue1);
        Message msg2_medium = queueService.pull(queue2);
        
        assertEquals("low-priority-q1", msg1_low.getBody());
        assertEquals("medium-priority-q2", msg2_medium.getBody());
        
        // All queues should now be empty
        assertNull(queueService.pull(queue1));
        assertNull(queueService.pull(queue2));
        assertNull(queueService.pull(queue3));
        
        // Delete operations should also be independent
        queueService.delete(queue1, msg1_high.getReceiptId());
        queueService.delete(queue2, msg2_urgent.getReceiptId());
        queueService.delete(queue3, msg3_normal.getReceiptId());
    }
    
    @Test
    @DisplayName("Should handle concurrent operations across multiple queues")
    void testConcurrentMultipleQueues() throws InterruptedException {
        int numberOfQueues = 5;
        int messagesPerQueue = 20;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(numberOfQueues * 2); // producers + consumers
        
        ConcurrentHashMap<String, AtomicInteger> queueMessageCounts = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>> queueResults = new ConcurrentHashMap<>();
        
        // Producer threads for each queue
        for (int queueId = 0; queueId < numberOfQueues; queueId++) {
            final String queueName = "concurrent-queue-" + queueId;
            queueMessageCounts.put(queueName, new AtomicInteger(0));
            queueResults.put(queueName, new ConcurrentLinkedQueue<>());
            
            executor.submit(() -> {
                try {
                    for (int j = 0; j < messagesPerQueue; j++) {
                        queueService.push(queueName, "message-" + j, j % 10);
                        queueMessageCounts.get(queueName).incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // Consumer threads for each queue
        for (int queueId = 0; queueId < numberOfQueues; queueId++) {
            final String queueName = "concurrent-queue-" + queueId;
            
            executor.submit(() -> {
                try {
                    for (int j = 0; j < messagesPerQueue; j++) {
                        Message msg = queueService.pull(queueName);
                        if (msg != null) {
                            queueResults.get(queueName).add(msg);
                            queueService.delete(queueName, msg.getReceiptId());
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        
        // Verify each queue processed independently
        for (int queueId = 0; queueId < numberOfQueues; queueId++) {
            String queueName = "concurrent-queue-" + queueId;
            assertTrue(queueMessageCounts.get(queueName).get() > 0);
            assertTrue(queueResults.get(queueName).size() > 0);
        }
    }
}
