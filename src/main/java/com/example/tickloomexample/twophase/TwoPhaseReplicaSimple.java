package com.example.tickloomexample.twophase;

import com.tickloom.ProcessId;
import com.tickloom.Replica;
import com.tickloom.messaging.AsyncQuorumCallback;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageBus;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;
import com.tickloom.messaging.RequestWaitingList;
import com.tickloom.network.MessageCodec;
import com.tickloom.storage.Storage;
import com.tickloom.storage.VersionedValue;
import com.tickloom.util.Clock;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import static com.example.tickloomexample.twophase.TwoPhaseMessages.*;

/**
 * Two-phase implementation using direct callbacks (no explicit coordinator state machine).
 * Useful to demonstrate a straightforward approach before introducing the coordinator pattern.
 */
public class TwoPhaseReplicaSimple extends Replica {

    private int counter = 0;
    private final Map<String, Boolean> acceptedLocally = new HashMap<>();
    private final Set<String> executedLocally = new HashSet<>();
    private final RequestWaitingList<String, Object> clientWaitingList;

    public TwoPhaseReplicaSimple(ProcessId id, List<ProcessId> peers, MessageBus bus, MessageCodec codec, Storage storage, Clock clock, int timeout) {
        super(id, peers, bus, codec, storage, clock, timeout);
        this.clientWaitingList = new RequestWaitingList<>(timeoutTicks);
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                CLIENT_INC_REQUEST, this::handleClientIncRequest,
                ACCEPT_REQUEST, this::handleAcceptRequest,
                ACCEPT_RESPONSE, this::handleAcceptResponse,
                EXECUTE_REQUEST, this::handleExecuteRequest
        );
    }

    private void handleClientIncRequest(Message msg) {
        ClientIncRequest req = deserializePayload(msg.payload(), ClientIncRequest.class);
        String corr = msg.correlationId();
        String requestId = req.requestId();

        // Register client wait – completed when local EXECUTE happens
        clientWaitingList.add(corr, createClientResponseCallback(msg.source(), corr, requestId));

        // Broadcast ACCEPTs and tie quorum success to EXECUTE broadcast
        broadcastAcceptRequestToAllReplicas(msg, requestId, corr);
    }

    private RequestCallback<Object> createClientResponseCallback(ProcessId client,
                                                                 String clientCorrelationId,
                                                                 String requestId) {
        return new RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                sendClientResponse(client, clientCorrelationId, requestId, true);
            }
            @Override
            public void onError(Exception error) {
                sendClientResponse(client, clientCorrelationId, requestId, false);
            }
        };
    }

    private void sendClientResponse(ProcessId client, String clientCorrelationId, String requestId, boolean acceptedByMajority) {
        try {
            int current = counter;
            var resp = new ClientIncResponse(requestId, acceptedByMajority, current);
            messageBus.sendMessage(createMessage(client, clientCorrelationId, resp, CLIENT_INC_RESPONSE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void broadcastAcceptRequestToAllReplicas(Message originalClientMessage,
                                                     String requestId,
                                                     String clientCorrelationId) {
        var acceptQuorum = new AsyncQuorumCallback<AcceptResponse>(
                getAllNodes().size(), r -> r != null && r.accepted());

        acceptQuorum
                .onSuccess(map -> broadcastExecuteToPeers(requestId, clientCorrelationId))
                .onFailure(err -> sendClientResponse(originalClientMessage.source(), clientCorrelationId, requestId, false));

        broadcastToAllReplicas(acceptQuorum, (peer, internalCorr) ->
                createMessage(peer, internalCorr, new AcceptRequest(requestId), ACCEPT_REQUEST));
    }

    // We intentionally reuse the client correlationId for EXECUTE fan-out for end-to-end linkage
    private void broadcastExecuteToPeers(String requestId, String clientCorrelationId) {
        for (ProcessId node : getAllNodes()) {
            try {
                messageBus.sendMessage(createMessage(node, clientCorrelationId, new ExecuteRequest(requestId), EXECUTE_REQUEST));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void handleAcceptRequest(Message msg) {
        AcceptRequest req = deserializePayload(msg.payload(), AcceptRequest.class);
        acceptedLocally.put(req.requestId(), true);
        send(createResponseMessage(msg, new AcceptResponse(req.requestId(), true), ACCEPT_RESPONSE));
    }

    private void handleAcceptResponse(Message msg) {
        AcceptResponse resp = deserializePayload(msg.payload(), AcceptResponse.class);
        waitingList.handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleExecuteRequest(Message msg) {
        ExecuteRequest req = deserializePayload(msg.payload(), ExecuteRequest.class);
        if (acceptedLocally.getOrDefault(req.requestId(), false) && !executedLocally.contains(req.requestId())) {
            counter = counter + 1;
            VersionedValue v = new VersionedValue(new byte[]{(byte) counter}, clock.now());
            storage.set(new byte[]{1}, v);
            executedLocally.add(req.requestId());
            clientWaitingList.handleResponse(msg.correlationId(), new ExecuteResponse(req.requestId(), true, counter), id);
            send(createResponseMessage(msg, new ExecuteResponse(req.requestId(), true, counter), EXECUTE_RESPONSE));
        } else {
            send(createResponseMessage(msg, new ExecuteResponse(req.requestId(), false, counter), EXECUTE_RESPONSE));
        }
    }
}


