# Paimon

Paimon is a data lake storage for streaming updates/deletes changelog ingestion and high-performance queries in real time.

Paimon is developed under the umbrella of [Apache Flink](https://flink.apache.org/).

## Documentation & Getting Started

Please check out the full [documentation](https://nightlies.apache.org/flink/paimon-docs-master/), hosted by the
[ASF](https://www.apache.org/), for detailed information and user guides.

Check our [quick-start](https://nightlies.apache.org/flink/paimon-docs-master/docs/try-paimon/quick-start/) guide for simple setup instructions to get you started with the paimon.

## Building

Run the `mvn clean package -DskipTests` command to build the project.

Then you will find a JAR file for Flink engine with all shaded dependencies: `paimon-flink/paimon-flink-**/target/paimon-flink-**-<version>.jar`.

## Contributing

You can learn more about how to contribute on the [Apache Flink website](https://flink.apache.org/contributing/how-to-contribute.html). For code contributions, please read carefully the [Contributing Code](https://flink.apache.org/contributing/contribute-code.html) section for an overview of ongoing community work.

## License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
