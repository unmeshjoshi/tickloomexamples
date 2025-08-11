package com.example.tickloomexample.echo;

import com.tickloom.Process;
import com.tickloom.ProcessId;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageBus;
import com.tickloom.messaging.MessageType;
import com.tickloom.network.MessageCodec;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.example.tickloomexample.echo.EchoMessages.*;

public class EchoServer extends Process {

    private final List<ProcessId> peerIds;

    public EchoServer(ProcessId id,
                      List<ProcessId> peerIds,
                      MessageBus messageBus,
                      MessageCodec messageCodec,
                      Storage storage,
                      Clock clock,
                      int timeoutTicks) {
        super(id, messageBus, messageCodec, timeoutTicks, clock);
        this.peerIds = peerIds;
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                ECHO_REQUEST, this::onEchoRequest
        );
    }

    private void onEchoRequest(Message msg) {
        EchoRequest request = deserializePayload(msg.payload(), EchoRequest.class);
        EchoResponse response = new EchoResponse(request.text());
        Message responseMessage = createResponseMessage(msg, response, ECHO_RESPONSE);
        try {
            messageBus.sendMessage(responseMessage);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}



