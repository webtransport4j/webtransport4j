# Contributing to webtransport4j

Thank you for your interest in contributing to **webtransport4j**!

We welcome contributions of all kinds: bug fixes, new features, performance optimizations, interoperability test cases, and documentation improvements.

Please review this guide before submitting your first pull request.

---

## Code of Conduct

This project adheres to the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

---

## Development Prerequisites

To build and contribute to `webtransport4j`, you will need:

* **Java Development Kit (JDK)**: JDK 17 or higher (JDK 21 LTS recommended; JDK 25 supported for Multi-Release JAR optimizations).
* **Apache Maven**: 3.8.0 or higher.
* **Git**: Modern version.
* *(Optional for Interop tests)*: Python 3.10+ (with `aioquic`) or Go 1.20+ (with `webtransport-go`).

---

## Project Structure

```
webtransport4j/
├── src/main/java/          # Core WebTransport server and API (Java 8 baseline)
│   └── io/github/webtransport4j/
│       ├── api/            # Public interfaces (Session, Stream, Datagram, Handler)
│       ├── server/         # Netty QUIC & HTTP/3 server implementation
│       ├── spring/         # Spring Boot AutoConfiguration & SmartLifecycle
│       └── quarkus/        # Quarkus / CDI integration
├── src/main/java-25/       # Java 25 Multi-Release JAR (MRJAR) Panama & MemorySegment optimizations
├── src/test/java/          # JUnit test suites & integration tests
├── src/jmh/                # JMH performance and throughput benchmarks
├── interop/                # Cross-language interop test scripts (Python, Go, Browser HTML)
└── config/                 # Static configuration and checkstyle definitions
```

---

## Building and Testing

### 1. Build the project
To compile the core sources:
```bash
mvn clean compile
```

### 2. Run the test suite
To run all unit and integration tests (includes Netty leak detection):
```bash
mvn clean test
```

### 3. Run a specific test
During development, you can run a single test class to save time:
```bash
mvn test -Dtest=FramingLayerTest
```

### 4. Running Benchmarks (JMH)
To run microbenchmarks via the `bench` Maven profile:
```bash
mvn test -Pbench
```

---

## Coding Guidelines

1. **Java Version Compatibility**:
   * Code in `src/main/java` must remain compatible with Java 8 (`<maven.compiler.release>8</maven.compiler.release>`). Do not use newer language features (e.g. `var`, records, switch expressions) in the baseline module.
   * Modern JVM optimizations (Panama Foreign Function & Memory API, `MemorySegment`) belong in `src/main/java-25` under the MRJAR profile.

2. **Netty Resource Management**:
   * Always properly release `ByteBuf` instances (`ReferenceCountUtil.release(msg)` or use auto-releasing handlers) to prevent native memory leaks.
   * Never execute blocking or long-running computations directly inside the Netty `EventLoop`. Use virtual threads or dedicated worker executors.

3. **Code Style**:
   * We follow Google Java Style.
   * Avoid wildcard imports (e.g., use `import java.util.List;` instead of `import java.util.*;`).
   * Keep lines wrapped within 120 characters where possible.

---

## Submitting a Pull Request

1. **Fork & Branch**: Create a feature branch off `main`:
   ```bash
   git checkout -b feat/my-new-feature
   ```
2. **Commit Conventions**: Use clear and descriptive commit messages (e.g., `feat: support capsule type X`, `fix: handle stream reset during handshake`).
3. **Verify Locally**: Make sure all tests compile and pass before pushing:
   ```bash
   mvn clean test
   ```
4. **Open PR**: Push your branch to your fork and submit a PR against `main`. Fill out the [Pull Request Template](.github/pull_request_template.md).
