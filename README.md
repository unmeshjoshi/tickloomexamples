## Distributed Systems Workshop (Java/Gradle)

### Overview
This repository contains small distributed-systems examples and exercises built with the TickLoom framework. It currently includes:
- A quorum-based key-value store: `src/main/java/com/distsys/quorumkv` with tests in `src/test/java/com/distsys/quorumkv/QuorumKVTest.java`
- A minimal echo server/client example: `src/main/java/com/example/tickloomexample/echo` with tests in `src/test/java/com/example/tickloomexample/echo/EchoClusterTest.java`

TickLoom artifacts used:
- Library: `io.github.unmeshjoshi:tickloom:0.1.0-alpha.4`
- Testkit: `io.github.unmeshjoshi:tickloom-testkit:0.1.0-alpha.4`

Reference: [TickLoom on Sonatype Central](https://central.sonatype.com/artifact/io.github.unmeshjoshi/tickloom)

### Requirements
- JDK 21+

### Build and test
```bash
./gradlew clean test
```

Run a specific test class:
```bash
./gradlew test --tests com.distsys.quorumkv.QuorumKVTest
```

If you prefer a system Gradle:
```bash
gradle clean test
```

### Project layout
- QuorumKV: `src/main/java/com/distsys/quorumkv/*`
- Echo example: `src/main/java/com/example/tickloomexample/echo/*`
- Perf utilities and tests: `src/test/java/com/example/perf/*`

### License
MIT (or your preferred license)
