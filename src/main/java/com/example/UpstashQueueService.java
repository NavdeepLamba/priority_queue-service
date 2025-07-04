package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Upstash Redis-based queue service implementation using HTTP REST API.
 * Implements priority queue functionality using Redis sorted sets.
 * Messages are stored with priority as score, and timestamp as tie-breaker for FIFO within same priority.
 */
public class UpstashQueueService implements QueueService {
    
    private final String upstashUrl;
    private final String upstashToken;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Map<String, Set<String>> invisibleMessages;
    private long visibilityTimeout;
    
    // Redis key prefixes for different data structures
    private static final String QUEUE_PREFIX = "queue:";
    private static final String INVISIBLE_PREFIX = "invisible:";
    private static final String MESSAGE_PREFIX = "msg:";

    public UpstashQueueService() {
        this.upstashUrl = System.getenv("UPSTASH_REDIS_REST_URL");
        this.upstashToken = System.getenv("UPSTASH_REDIS_REST_TOKEN");
        
        if (upstashUrl == null || upstashToken == null) {
            throw new IllegalArgumentException("Upstash credentials not found in environment variables");
        }
        
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
        this.objectMapper = new ObjectMapper();
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
        push(queueUrl, msgBody, 0);
    }
    
    /**
     * Push a message with specified priority to the Redis-based queue.
     * Uses sorted sets with priority as primary score and timestamp as secondary score.
     */
    @Override
    public void push(String queueUrl, String msgBody, int priority) {
        try {
            String messageId = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();
            
            // Create composite score: priority (high 32 bits) + inverted timestamp (low 32 bits) for FIFO within priority
            // Higher priority gets higher score, older messages get higher score within same priority
            double score = ((double) priority * 1_000_000_000L) + (Long.MAX_VALUE - timestamp);
            
            // Store message content
            String messageKey = MESSAGE_PREFIX + messageId;
            executeRedisCommand("HSET", messageKey, "body", msgBody, "priority", String.valueOf(priority), "timestamp", String.valueOf(timestamp));
            
            // Add to sorted set queue
            String queueKey = QUEUE_PREFIX + queueUrl;
            executeRedisCommand("ZADD", queueKey, String.valueOf(score), messageId);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to push message to queue: " + queueUrl, e);
        }
    }

    @Override
    public Message pull(String queueUrl) {
        try {
            String queueKey = QUEUE_PREFIX + queueUrl;
            
            // Clean up expired invisible messages first
            cleanupExpiredMessages(queueUrl);
            
            // Get highest priority message (highest score)
            JsonNode response = executeRedisCommand("ZREVRANGE", queueKey, "0", "0", "WITHSCORES");
            if (response == null || !response.isArray() || response.size() == 0) {
                return null;
            }
            
            String messageId = response.get(0).asText();
            
            // Remove from queue atomically
            Long removed = executeRedisCommand("ZREM", queueKey, messageId).asLong();
            if (removed == 0) {
                // Message was already consumed by another consumer
                return null;
            }
            
            // Get message content
            String messageKey = MESSAGE_PREFIX + messageId;
            JsonNode messageData = executeRedisCommand("HMGET", messageKey, "body", "priority", "timestamp");
            
            if (messageData == null || !messageData.isArray() || messageData.size() < 3) {
                return null;
            }
            
            String body = messageData.get(0).asText();
            int priority = messageData.get(1).asInt();
            long timestamp = messageData.get(2).asLong();
            
            // Generate receipt ID and set visibility timeout
            String receiptId = UUID.randomUUID().toString();
            long visibleFrom = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout);
            
            // Store invisible message with TTL
            String invisibleKey = INVISIBLE_PREFIX + queueUrl + ":" + receiptId;
            executeRedisCommand("HSET", invisibleKey, "messageId", messageId, "body", body, "priority", String.valueOf(priority), "timestamp", String.valueOf(timestamp));
            executeRedisCommand("EXPIRE", invisibleKey, String.valueOf(visibilityTimeout + 10)); // Add buffer for cleanup
            
            // Track invisible message locally
            Set<String> invisible = invisibleMessages.computeIfAbsent(queueUrl, k -> ConcurrentHashMap.newKeySet());
            invisible.add(receiptId);
            
            return new Message(body, receiptId, priority);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to pull message from queue: " + queueUrl, e);
        }
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        try {
            String invisibleKey = INVISIBLE_PREFIX + queueUrl + ":" + receiptId;
            
            // Get message details before deletion
            JsonNode messageData = executeRedisCommand("HMGET", invisibleKey, "messageId");
            if (messageData != null && messageData.isArray() && messageData.size() > 0) {
                String messageId = messageData.get(0).asText();
                
                // Delete message content
                String messageKey = MESSAGE_PREFIX + messageId;
                executeRedisCommand("DEL", messageKey);
            }
            
            // Delete invisible message
            executeRedisCommand("DEL", invisibleKey);
            
            // Remove from local tracking
            Set<String> invisible = invisibleMessages.get(queueUrl);
            if (invisible != null) {
                invisible.remove(receiptId);
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete message from queue: " + queueUrl, e);
        }
    }
    
    /**
     * Clean up expired invisible messages by making them visible again
     */
    private void cleanupExpiredMessages(String queueUrl) {
        try {
            Set<String> invisible = invisibleMessages.get(queueUrl);
            if (invisible == null) {
                return;
            }
            
            Iterator<String> iterator = invisible.iterator();
            while (iterator.hasNext()) {
                String receiptId = iterator.next();
                String invisibleKey = INVISIBLE_PREFIX + queueUrl + ":" + receiptId;
                
                // Check if message still exists (not expired)
                JsonNode exists = executeRedisCommand("EXISTS", invisibleKey);
                if (exists == null || exists.asInt() == 0) {
                    // Message expired, remove from tracking
                    iterator.remove();
                    continue;
                }
                
                // Check TTL to see if we should restore the message
                JsonNode ttl = executeRedisCommand("TTL", invisibleKey);
                if (ttl != null && ttl.asLong() <= 10) { // Within buffer time
                    // Get message data and restore to queue
                    JsonNode messageData = executeRedisCommand("HMGET", invisibleKey, "messageId", "body", "priority", "timestamp");
                    if (messageData != null && messageData.isArray() && messageData.size() >= 4) {
                        String messageId = messageData.get(0).asText();
                        String body = messageData.get(1).asText();
                        int priority = messageData.get(2).asInt();
                        long timestamp = messageData.get(3).asLong();
                        
                        // Restore to queue
                        double score = ((double) priority * 1_000_000_000L) + (Long.MAX_VALUE - timestamp);
                        String queueKey = QUEUE_PREFIX + queueUrl;
                        executeRedisCommand("ZADD", queueKey, String.valueOf(score), messageId);
                        
                        // Clean up invisible message
                        executeRedisCommand("DEL", invisibleKey);
                        iterator.remove();
                    }
                }
            }
        } catch (Exception e) {
            // Log error but don't fail the operation
            System.err.println("Error cleaning up expired messages: " + e.getMessage());
        }
    }
    
    /**
     * Execute Redis command via HTTP REST API
     */
    private JsonNode executeRedisCommand(String... args) throws IOException, InterruptedException {
        List<String> command = Arrays.asList(args);
        String jsonCommand = objectMapper.writeValueAsString(command);
        
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(upstashUrl))
            .header("Authorization", "Bearer " + upstashToken)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonCommand))
            .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
            throw new RuntimeException("Redis command failed with status: " + response.statusCode() + ", body: " + response.body());
        }
        
        JsonNode jsonResponse = objectMapper.readTree(response.body());
        
        // Check for Redis error
        if (jsonResponse.has("error")) {
            throw new RuntimeException("Redis error: " + jsonResponse.get("error").asText());
        }
        
        return jsonResponse.get("result");
    }
}
