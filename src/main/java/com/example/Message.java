package com.example;

public class Message {
    /** How many times this message has been delivered. */
    private int attempts;
    
    /** Visible from time */
    private long visibleFrom;
    
    /** An identifier associated with the act of receiving the message. */
    private String receiptId;
    
    private String msgBody;
    
    /** Priority of the message - higher numbers indicate higher priority */
    private int priority;
    
    /** Timestamp when the message was created for FIFO ordering within same priority */
    private long timestamp;

    // Default constructor for non-priority messages
    Message(String msgBody) {
        this.msgBody = msgBody;
        this.priority = 0; // Default priority
        this.timestamp = System.currentTimeMillis();
    }

    // Constructor with priority support
    Message(String msgBody, int priority) {
        this.msgBody = msgBody;
        this.priority = priority;
        this.timestamp = System.currentTimeMillis();
    }

    Message(String msgBody, String receiptId) {
        this.msgBody = msgBody;
        this.receiptId = receiptId;
        this.priority = 0; // Default priority
        this.timestamp = System.currentTimeMillis();
    }
    
    // Constructor with all parameters
    Message(String msgBody, String receiptId, int priority) {
        this.msgBody = msgBody;
        this.receiptId = receiptId;
        this.priority = priority;
        this.timestamp = System.currentTimeMillis();
    }

    public String getReceiptId() {
        return this.receiptId;
    }

    protected void setReceiptId(String receiptId) {
        this.receiptId = receiptId;
    }

    protected void setVisibleFrom(long visibleFrom) {
        this.visibleFrom = visibleFrom;
    }

    public boolean isVisibleAt(long instant) {
        return visibleFrom < instant;
    }

    public String getBody() {
        return msgBody;
    }

    protected int getAttempts() {
        return attempts;
    }

    protected void incrementAttempts() {
        this.attempts++;
    }
    
    public int getPriority() {
        return priority;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    protected void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
