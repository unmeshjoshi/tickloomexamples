package com.example.tickloomexample.echo;

import com.tickloom.ProcessId;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.MessageBus;
import com.tickloom.messaging.MessageType;
import com.tickloom.network.MessageCodec;
import com.tickloom.util.Clock;

import java.util.List;

import static com.example.tickloomexample.echo.EchoMessages.*;

public class EchoClient extends ClusterClient {

    public EchoClient(ProcessId clientId,
                      List<ProcessId> replicaEndpoints,
                      MessageBus messageBus,
                      MessageCodec messageCodec,
                      Clock clock,
                      int timeoutTicks) {
        super(clientId, replicaEndpoints, messageBus, messageCodec, clock, timeoutTicks);
    }

    public ListenableFuture<EchoResponse> echo(ProcessId server, String text) {
        EchoRequest req = new EchoRequest(text);
        return sendRequest(req, server, ECHO_REQUEST);
    }

    @Override
    protected java.util.Map<MessageType, Handler> initialiseHandlers() {
        return java.util.Map.of(
                ECHO_RESPONSE, msg -> {
                    EchoResponse resp = deserialize(msg.payload(), EchoResponse.class);
                    handleResponse(msg.correlationId(), resp, msg.source());
                }
        );
    }
}



