## TickLoom Examples (Java/Gradle)

### Overview
This project demonstrates building small distributed examples on top of TickLoom:
- Using the SimulatedNetwork and Cluster testkit
- Modeling a two-phase (accept → execute) flow
- Injecting network faults by message type for scenario tests

Artifacts used:
- Library: `io.github.unmeshjoshi:tickloom:0.1.0-alpha.3`
- Testkit: `io.github.unmeshjoshi:tickloom-testkit:0.1.0-alpha.3`

Reference: [TickLoom on Sonatype Central](https://central.sonatype.com/artifact/io.github.unmeshjoshi/tickloom)

### What’s included
- Echo example (minimal server/client):
  - `src/main/java/com/example/tickloomexample/echo/*`
  - `src/test/java/com/example/tickloomexample/echo/EchoClusterTest.java`

- Two-phase execution examples (increment counter):
  - Coordinator-based (explicit state machine):
    - `src/main/java/com/example/tickloomexample/twophase/TwoPhaseReplica.java`
  - Simple direct-callback style:
    - `src/main/java/com/example/tickloomexample/twophase/TwoPhaseReplicaSimple.java`
  - Shared messages:
    - `src/main/java/com/example/tickloomexample/twophase/TwoPhaseMessages.java`
  - Scenario tests (success + fault injection):
    - `src/test/java/com/example/tickloomexample/twophase/TwoPhaseExecutionTest.java`

### Fault injection
TickLoom alpha.3 supports dropping messages between specific processes by message type, e.g.:
- `SimulatedNetwork.dropMessagesOfType(source, destination, messageType)`

The tests use this to model:
- two_phase_lost_commits: drop `EXECUTE_REQUEST`
- two_phase_missed_accepts: drop `ACCEPT_REQUEST`

### Design notes
- We reuse the client correlationId for EXECUTE fan-out to preserve end-to-end traceability (client → accept → execute → client response).
- We keep client waits isolated using a separate `RequestWaitingList` to avoid interaction with internal waits.
- The coordinator-based version wraps the flow in a small per-request state machine with a single unified callback for clarity and extensibility.

### Requirements
- JDK 17+

### Run tests
```bash
./gradlew test
```

If you prefer a system Gradle:
```bash
gradle test
```

### Project structure (key paths)
- Build: `build.gradle` (Gradle Java + JUnit 5)
- Echo: `com.example.tickloomexample.echo.*`
- Two-phase (simple + coordinator): `com.example.tickloomexample.twophase.*`


