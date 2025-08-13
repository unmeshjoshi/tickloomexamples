package com.distsys.quorumkv;

import com.tickloom.ProcessId;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.algorithms.replication.quorum.GetRequest;
import com.tickloom.algorithms.replication.quorum.GetResponse;
import com.tickloom.algorithms.replication.quorum.QuorumMessageTypes;
import com.tickloom.algorithms.replication.quorum.SetRequest;
import com.tickloom.algorithms.replication.quorum.SetResponse;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageBus;
import com.tickloom.messaging.MessageType;
import com.tickloom.network.MessageCodec;
import com.tickloom.util.Clock;

import java.util.List;
import java.util.Map;

public class QuorumKVClient extends ClusterClient {
    
    public QuorumKVClient(ProcessId clientId, List<ProcessId> replicaEndpoints,
                          MessageBus messageBus, MessageCodec messageCodec,
                          Clock clock, int timeoutTicks) {
        super(clientId, replicaEndpoints, messageBus, messageCodec, clock, timeoutTicks);
    }
    
    public ListenableFuture<com.tickloom.algorithms.replication.quorum.GetResponse> get(byte[] key) {
        GetRequest request = new GetRequest(key);
        ProcessId primaryReplica = getPrimaryReplica();
        
        return sendRequest(request, primaryReplica, QuorumMessageTypes.CLIENT_GET_REQUEST);
    }
    
    public ListenableFuture<com.tickloom.algorithms.replication.quorum.SetResponse> set(byte[] key, byte[] value) {
        com.tickloom.algorithms.replication.quorum.SetRequest request = new SetRequest(key, value);
        ProcessId primaryReplica = getPrimaryReplica();
        
        return sendRequest(request, primaryReplica, QuorumMessageTypes.CLIENT_SET_REQUEST);
    }


    private void handleSetResponse(Message message) {
        com.tickloom.algorithms.replication.quorum.SetResponse response = deserialize(message.payload(), SetResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }

    private void handleGetResponse(Message message) {
        com.tickloom.algorithms.replication.quorum.GetResponse response = deserialize(message.payload(), GetResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                QuorumMessageTypes.CLIENT_GET_RESPONSE, this::handleGetResponse,
                QuorumMessageTypes.CLIENT_SET_RESPONSE, this::handleSetResponse);

    }

    private ProcessId getPrimaryReplica() {
        return replicaEndpoints.get(0);
    }
}
