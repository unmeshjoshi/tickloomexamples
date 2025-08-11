package com.example.tickloomexample.twophase;

import com.tickloom.ProcessId;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.MessageBus;
import com.tickloom.messaging.MessageType;
import com.tickloom.network.JsonMessageCodec;
import com.tickloom.network.MessageCodec;
import com.tickloom.network.Network;
import com.tickloom.network.SimulatedNetwork;
import com.tickloom.storage.SimulatedStorage;
import com.tickloom.storage.Storage;
import com.tickloom.testkit.Cluster;
import com.tickloom.util.Clock;
import com.tickloom.util.SystemClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static com.example.tickloomexample.twophase.TwoPhaseMessages.*;
import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TwoPhaseExecutionTest {

    private Cluster cluster;

    static class TwoPhaseClient extends ClusterClient {
        public TwoPhaseClient(ProcessId clientId, List<ProcessId> replicas, MessageBus bus, MessageCodec codec, Clock clock, int timeout) {
            super(clientId, replicas, bus, codec, clock, timeout);
        }

        @Override
        protected java.util.Map<com.tickloom.messaging.MessageType, Handler> initialiseHandlers() {
            return java.util.Map.of(
                    CLIENT_INC_RESPONSE, msg -> {
                        ClientIncResponse resp = deserialize(msg.payload(), ClientIncResponse.class);
                        handleResponse(msg.correlationId(), resp, msg.source());
                    }
            );
        }

        public ListenableFuture<ClientIncResponse> inc(ProcessId server, String rid) {
            return sendRequest(new ClientIncRequest(rid), server, CLIENT_INC_REQUEST);
        }
    }

    @BeforeEach
    void setup() throws Exception {
        cluster = new Cluster()
                .withNumProcesses(3)
                .useSimulatedNetwork()
                .build(TwoPhaseReplica::new)
                .start();
    }

    @AfterEach
    void teardown() { if (cluster != null) cluster.close(); }

    @Test
    void two_phase_execution_success() throws Exception {
        ProcessId s1 = ProcessId.of("process-1");
        TwoPhaseClient client = cluster.newClient(ProcessId.of("client-1"), (id, endpoints, bus, codec, clock, t) ->
                new TwoPhaseClient(id, List.of(s1), bus, codec, clock, t));

        var f = client.inc(s1, "req-1");
        assertEventually(cluster, f::isCompleted);
        assertEquals(1, f.getResult().committedValue());

        // All nodes should have executed: counter == 1 on all
        byte[] key = "counter".getBytes();
        var v1 = cluster.getStorageValue(s1, key);
        var v2 = cluster.getStorageValue(ProcessId.of("process-2"), key);
        var v3 = cluster.getStorageValue(ProcessId.of("process-3"), key);
        org.junit.jupiter.api.Assertions.assertNotNull(v1);
        org.junit.jupiter.api.Assertions.assertNotNull(v2);
        org.junit.jupiter.api.Assertions.assertNotNull(v3);
        org.junit.jupiter.api.Assertions.assertEquals(1, v1.value()[0]);
        org.junit.jupiter.api.Assertions.assertEquals(1, v2.value()[0]);
        org.junit.jupiter.api.Assertions.assertEquals(1, v3.value()[0]);
    }

    @Test
    void two_phase_lost_commits_execute_messages_lost() throws Exception {
        ProcessId s1 = ProcessId.of("process-1");
        ProcessId s2 = ProcessId.of("process-2");
        ProcessId s3 = ProcessId.of("process-3");

        cluster.dropMessagesOfType(s1, s2, EXECUTE_REQUEST);
        cluster.dropMessagesOfType(s1, s3, EXECUTE_REQUEST);

        TwoPhaseClient client = cluster.newClient(ProcessId.of("client-1"), (id, endpoints, bus, codec, clock, t) ->
                new TwoPhaseClient(id, List.of(s1), bus, codec, clock, t));

        var f = client.inc(s1, "req-2");
        assertEventually(cluster, f::isCompleted);
        // Only s1 should have executed; value should still be 1 at s1
        assertEquals(1, f.getResult().committedValue());

        // Validate that replicas which missed EXECUTE did not change their state
        byte[] key = "counter".getBytes();
        var v1 = cluster.getStorageValue(s1, key);
        var v2 = cluster.getStorageValue(s2, key);
        var v3 = cluster.getStorageValue(s3, key);
        org.junit.jupiter.api.Assertions.assertNotNull(v1);
        org.junit.jupiter.api.Assertions.assertEquals(1, v1.value()[0]);
        // Both should be null because they never executed
        org.junit.jupiter.api.Assertions.assertNull(v2);
        org.junit.jupiter.api.Assertions.assertNull(v3);
    }

    @Test
    void two_phase_missed_accepts_accept_messages_lost() throws Exception {
        ProcessId s1 = ProcessId.of("process-1");
        ProcessId s2 = ProcessId.of("process-2");
        ProcessId s3 = ProcessId.of("process-3");

        // Arrange: drop ACCEPT messages from s1 to s2
        cluster.dropMessagesOfType(s1, s2, ACCEPT_REQUEST);

        TwoPhaseClient client = cluster.newClient(ProcessId.of("client-1"), (id, endpoints, bus, codec, clock, t) ->
                new TwoPhaseClient(id, List.of(s1), bus, codec, clock, t));

        var f = client.inc(s1, "req-3");
        assertEventually(cluster, f::isCompleted);
        // Fresh cluster per test: after one increment, value should be 1
        assertEquals(1, f.getResult().committedValue());

        // Validate that the node which missed ACCEPT did not execute, others did
        byte[] key = "counter".getBytes();
        var v1 = cluster.getStorageValue(s1, key);
        var v2 = cluster.getStorageValue(s2, key);
        var v3 = cluster.getStorageValue(s3, key);
        org.junit.jupiter.api.Assertions.assertNotNull(v1);
        org.junit.jupiter.api.Assertions.assertNull(v2);
        org.junit.jupiter.api.Assertions.assertNotNull(v3);
        org.junit.jupiter.api.Assertions.assertEquals(1, v1.value()[0]);
        org.junit.jupiter.api.Assertions.assertEquals(1, v3.value()[0]);
    }
}


