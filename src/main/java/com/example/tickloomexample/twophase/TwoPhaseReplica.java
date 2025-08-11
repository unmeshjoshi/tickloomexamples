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
import java.util.*;

import static com.example.tickloomexample.twophase.TwoPhaseMessages.*;

public class TwoPhaseReplica extends Replica {

    private int counter = 0;
    private final Map<String, Boolean> acceptedLocally = new HashMap<>();
    private final Set<String> executedLocally = new HashSet<>();
    // Separate waiting list for client responses to avoid interaction with internal waits
    private final RequestWaitingList<String, Object> clientWaitingList;
    // Track coordinators per client correlation id
    private final Map<String, RequestCoordinator> requestCoordinators = new HashMap<>();

    public TwoPhaseReplica(ProcessId id, List<ProcessId> peers, MessageBus bus, MessageCodec codec, Storage storage, Clock clock, int timeout) {
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
        RequestCoordinator coordinator = new RequestCoordinator(msg.source(), corr, requestId);
        requestCoordinators.put(corr, coordinator);
        coordinator.start();
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

     // Decision: reuse the client correlationId for the internal EXECUTE flow as well.
        // Rationale:
        // - It gives end-to-end linkage: client request -> ACCEPT quorum -> EXECUTE -> client response.
        // - Correlation IDs are GUIDs, so collision risk is negligible.
        // - We keep client waits isolated via clientWaitingList (separate from internal waits),
        //   so there is no key-space contention with replica-internal correlation IDs.
        // The callback below is completed when this node processes its local EXECUTE.
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
            storage.set("counter".getBytes(), v);
            executedLocally.add(req.requestId());
            // Complete the client callback registered under the client correlationId
            clientWaitingList.handleResponse(msg.correlationId(), new ExecuteResponse(req.requestId(), true, counter), id);
            send(createResponseMessage(msg, new ExecuteResponse(req.requestId(), true, counter), EXECUTE_RESPONSE));
        } else {
            send(createResponseMessage(msg, new ExecuteResponse(req.requestId(), false, counter), EXECUTE_RESPONSE));
        }
    }

    // Per-request coordinator with explicit phases
    private final class RequestCoordinator {
        private enum Phase { ACCEPTING, EXECUTING, COMPLETED }
        private enum Signal { ACCEPT_OK, ACCEPT_FAIL, EXECUTE_LOCAL_OK, EXECUTE_LOCAL_FAIL }

        private final ProcessId client;
        private final String clientCorrelationId;
        private final String requestId;
        private Phase phase = Phase.ACCEPTING;

        RequestCoordinator(ProcessId client, String clientCorrelationId, String requestId) {
            this.client = client;
            this.clientCorrelationId = clientCorrelationId;
            this.requestId = requestId;
        }

        void start() {
            // Complete when local EXECUTE happens
            clientWaitingList.add(clientCorrelationId, unifiedCallback());
            sendAcceptToAll();
        }

        private void sendAcceptToAll() {
            var acceptQuorum = new AsyncQuorumCallback<AcceptResponse>(
                    getAllNodes().size(), r -> r != null && r.accepted());

            acceptQuorum
                    .onSuccess(map -> handle(Signal.ACCEPT_OK))
                    .onFailure(err -> handle(Signal.ACCEPT_FAIL));

            broadcastToAllReplicas(acceptQuorum, (peer, internalCorr) ->
                    createMessage(peer, internalCorr, new AcceptRequest(requestId), ACCEPT_REQUEST));
        }

        private RequestCallback<Object> unifiedCallback() {
            return new RequestCallback<Object>() {
                @Override
                public void onResponse(Object response, ProcessId fromNode) {
                    handle(Signal.EXECUTE_LOCAL_OK);
                }

                @Override
                public void onError(Exception error) {
                    handle(Signal.EXECUTE_LOCAL_FAIL);
                }
            };
        }

        private void tryCleanup() {
            requestCoordinators.remove(clientCorrelationId);
        }

        private void handle(Signal signal) {
            switch (phase) {
                case ACCEPTING -> {
                    if (signal == Signal.ACCEPT_OK) {
                        phase = Phase.EXECUTING;
                        // Broadcast EXECUTE; local delivery will drive the callback into EXECUTE_LOCAL_OK
                        broadcastExecuteToPeers(requestId, clientCorrelationId);
                    } else if (signal == Signal.ACCEPT_FAIL) {
                        phase = Phase.COMPLETED;
                        tryCleanup();
                        sendClientResponse(client, clientCorrelationId, requestId, false);
                    }
                }
                case EXECUTING -> {
                    if (signal == Signal.EXECUTE_LOCAL_OK) {
                        phase = Phase.COMPLETED;
                        tryCleanup();
                        sendClientResponse(client, clientCorrelationId, requestId, true);
                    } else if (signal == Signal.EXECUTE_LOCAL_FAIL) {
                        phase = Phase.COMPLETED;
                        tryCleanup();
                        sendClientResponse(client, clientCorrelationId, requestId, false);
                    }
                }
                case COMPLETED -> {
                    // no-op
                }
            }
        }
    }
}


