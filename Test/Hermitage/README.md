```markdown
# Hermitage Test for VeloxDB

[Hermitage](https://github.com/ept/hermitage) is a database test suite designed to evaluate database isolation levels. It identifies various anomalies, and based on which anomalies are observed, you can determine the isolation level the database supports. Since VeloxDB supports only the serializable isolation level, no anomalies should be observed during these tests.

## A Few Notes About the Implementation

VeloxDB does not support interactive mode. Therefore, this implementation uses VeloxDB in embedded mode, allowing for precise control over transaction execution. With embedded mode, we can initiate multiple transactions in parallel and control when each commits. The engine used in embedded VeloxDB is the same as in regular VeloxDB, ensuring that all isolation guarantees are consistent.

## Running the Test

The tests are written using the NUnit framework, with each test targeting a specific anomaly. To run the tests, you will need .NET 8 installed on your machine.

### Running the Tests from the Command Line

You can run the tests by navigating to the project directory and executing:

```bash
dotnet test
```

Alternatively, you can run the tests using Visual Studio’s Test Explorer if you prefer a graphical interface.

### Running the Tests in the Browser with GitHub Codespaces

If you don't have .NET installed locally, you can run the tests directly in your browser using GitHub Codespaces. Click the button below to get started:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/VeloxDB/VeloxDB)

## Debugging the Test

You can attach a debugger to the tests to inspect their execution and observe what's happening in real time. This can be useful for better understanding VeloxDB hermitage implementation.

### Debuging in Codespaces

If running in codespaces, you can use testing tab in activity bar on the right side of the screen.

