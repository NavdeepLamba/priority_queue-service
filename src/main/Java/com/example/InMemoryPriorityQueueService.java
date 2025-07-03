package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * In-memory priority queue implementation that supports priority-based message delivery.
 * Messages with higher priority values are delivered first, with FIFO ordering within the same priority level.
 * Thread-safe implementation using PriorityBlockingQueue and ConcurrentHashMap.
 */
public class InMemoryPriorityQueueService implements QueueService {
    
    private final Map<String, PriorityBlockingQueue<Message>> queues;
    private final Map<String, Set<Message>> invisibleMessages;
    private long visibilityTimeout;

    public InMemoryPriorityQueueService() {
        this.queues = new ConcurrentHashMap<>();
        this.invisibleMessages = new ConcurrentHashMap<>();
        
        // Load configuration
        String propFileName = "config.properties";
        Properties confInfo = new Properties();
        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
            if (inStream != null) {
                confInfo.load(inStream);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
    }

    @Override
    public void push(String queueUrl, String msgBody) {
        // Push with default priority 0
        push(queueUrl, msgBody, 0);
    }
    
    /**
     * Push a message with specified priority to the queue.
     * Higher priority values are processed first.
     */
    public void push(String queueUrl, String msgBody, int priority) {
        PriorityBlockingQueue<Message> queue = queues.computeIfAbsent(queueUrl, 
            k -> new PriorityBlockingQueue<>(11, new MessageComparator()));
        
        Message message = new Message(msgBody, priority);
        queue.offer(message);
    }

    @Override
    public Message pull(String queueUrl) {
        PriorityBlockingQueue<Message> queue = queues.get(queueUrl);
        if (queue == null) {
            return null;
        }

        long nowTime = now();
        Set<Message> invisible = invisibleMessages.computeIfAbsent(queueUrl, k -> ConcurrentHashMap.newKeySet());
        
        // Clean up expired invisible messages first
        cleanupExpiredMessages(queue, invisible, nowTime);
        
        Message msg = queue.poll();
        if (msg == null) {
            return null;
        }

        // Set up visibility timeout
        String receiptId = UUID.randomUUID().toString();
        msg.setReceiptId(receiptId);
        msg.incrementAttempts();
        msg.setVisibleFrom(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout));
        
        // Add to invisible messages
        invisible.add(msg);
        
        return new Message(msg.getBody(), msg.getReceiptId(), msg.getPriority());
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        Set<Message> invisible = invisibleMessages.get(queueUrl);
        if (invisible != null) {
            invisible.removeIf(msg -> receiptId.equals(msg.getReceiptId()));
        }
    }
    
    /**
     * Clean up expired invisible messages by making them visible again
     */
    private void cleanupExpiredMessages(PriorityBlockingQueue<Message> queue, Set<Message> invisible, long nowTime) {
        Iterator<Message> iterator = invisible.iterator();
        while (iterator.hasNext()) {
            Message msg = iterator.next();
            if (msg.isVisibleAt(nowTime)) {
                // Message visibility timeout expired, make it available again
                msg.setReceiptId(null);
                msg.setVisibleFrom(0);
                queue.offer(msg);
                iterator.remove();
            }
        }
    }

    long now() {
        return System.currentTimeMillis();
    }

    /**
     * Custom comparator for priority queue ordering.
     * Higher priority values come first, with FIFO ordering for same priority using timestamp.
     */
    private static class MessageComparator implements Comparator<Message> {
        @Override
        public int compare(Message m1, Message m2) {
            // Higher priority first (descending order)
            int priorityComparison = Integer.compare(m2.getPriority(), m1.getPriority());
            if (priorityComparison != 0) {
                return priorityComparison;
            }
            // Same priority: FIFO order using timestamp (ascending order)
            return Long.compare(m1.getTimestamp(), m2.getTimestamp());
        }
    }
}
