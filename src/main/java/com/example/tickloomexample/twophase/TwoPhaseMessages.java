package com.example.tickloomexample.twophase;

import com.tickloom.messaging.MessageType;

public final class TwoPhaseMessages {
    private TwoPhaseMessages() {}

    public static final MessageType CLIENT_INC_REQUEST = MessageType.of("CLIENT_INC_REQUEST");
    public static final MessageType CLIENT_INC_RESPONSE = MessageType.of("CLIENT_INC_RESPONSE");

    public static final MessageType ACCEPT_REQUEST = MessageType.of("ACCEPT_REQUEST");
    public static final MessageType ACCEPT_RESPONSE = MessageType.of("ACCEPT_RESPONSE");

    public static final MessageType EXECUTE_REQUEST = MessageType.of("EXECUTE_REQUEST");
    public static final MessageType EXECUTE_RESPONSE = MessageType.of("EXECUTE_RESPONSE");

    public static record ClientIncRequest(String requestId) {}
    public static record ClientIncResponse(String requestId, boolean success, int committedValue) {}

    public static record AcceptRequest(String requestId) {}
    public static record AcceptResponse(String requestId, boolean accepted) {}

    public static record ExecuteRequest(String requestId) {}
    public static record ExecuteResponse(String requestId, boolean executed, int value) {}
}
