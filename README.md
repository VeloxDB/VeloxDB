[![Discord](https://img.shields.io/discord/950447971022028932?label=discord&logo=discord&style=flat-square&logoColor=white&color=7289DA)](https://discord.gg/E45qUQtrtx)

# VeloxDB - High performance OO database for .NET

VeloxDB is a high performance, in-memory, object oriented database. It was designed from the ground up with low latency and high throughput in mind. It, however, does not sacrifice the ease of use to achieve this. With full ACID compliance, developing high performance transactional applications should be as simple as possible.

# Features

* High performance and low cost - 2.5 million write transactions per second at a lower cost compared to competitors
* Strong consistency guarantees - strict serializability for read and read-write transactions
* High availability - synchronous replication to ensure data durability and availability
* Scale-out for reads - unlimited number of read-only replicas
* Schema-ful, object-oriented design - user-defined data model and business logic in .NET
* In-memory - entire data set must fit in system memory, allowing for fast access and low latency operations while still being persisted on disk for durability
* Specialized inverse reference indexing - optimized for fast navigation of inverse references
* Merging of data and logic - business logic executes in the same process as the database

## Getting started

[The VeloxDB Getting Started Guide](https://vlxdb.com/doc/get_started.html) - the perfect place to begin if you're new to VeloxDB. It covers everything from setting up your development environment to running your first transactions.

[VeloxDB: The Definitive Guide](https://vlxdb.com/guide/introduction.html) - a comprehensive resource that covers all aspects of using VeloxDB. It includes detailed information about the database's data model, API, as well as best practices for using VeloxDB in your applications.

[The VeloxDB API Reference](https://vlxdb.com/api) - a complete reference for the VeloxDB API, including detailed descriptions of all the available classes, methods, and properties. You can find the API Reference here:

## Building

Clone the repository, and run from the repository root
```
dotnet build /p:Platform=x64
```

## License

VeloxDB is licensed under [MIT License](LICENSE.txt).

## Contact

If you have any questions or feedback about VeloxDB, please don't hesitate to reach out to us. Here are a few ways to get in touch:
  * Use the Github issue tracker to report bugs or request features
  * [Join our Discord](https://discord.gg/E45qUQtrtx) community to ask questions and get help from other VeloxDB users
  * Contact us by email at support@vlxdb.com