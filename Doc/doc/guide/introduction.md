---
uid: introduction
---
# Introduction
VeloxDB is a high performance, in-memory, object oriented database. It was designed from the ground up with low latency and high throughput in mind. It, however, does not sacrifice the ease of use to achieve this. With full ACID compliance, developing high performance transactional applications should be as simple as possible. Having said all this, VeloxDB is not a silver bullet. It aims to be a tool for a specific subset of problems and to excel in those. The remainder of this chapter elaborates in more details why VeloxDB exists and what you should and shouldn't expect from it, allowing you to make an educated decision whether this is the right database for your specific needs.

## Why Another Database
There are already a large number of databases on the market today. Besides traditional relational databases, numerous other choices have emerged in the last two decades. When having to choose a database for a specific solution, it is essential to have a solid understanding of different criteria that differentiates those databases between each other, some of those being:
* **Consistency guarantees** - this is a very complex topic. A very good read on the topic can be found at [Jepsen](https://jepsen.io/consistency). To put it simply, consistency guarantees directly influence whether the database is easy or hard to use. In our own opinion, serializable isolation is essential if you want your business logic to be clear and simple without having hidden, difficult to find bugs (that usually only appear in production). Many modern database systems make it extremely difficult (purposely or not) to fully understand what consistency guarantees are available. This is often caused by the fact that not all use cases offer the same guarantees (like, for example, multi-value transactions in some databases, indexes in others) or that some use cases significantly degrade performance to retain strong guarantees.
* **Transaction throughput** - this essentially relates to the level of performance you can expect when performing certain types of operations on the database, usually expressed in the number of read and/or write transactions executed per second. This number varies significantly between databases. In typical CRUD (create, read, update, delete) scenarios, where a single, or at most a few entities are processed per transaction, traditional relational databases achieve throughput in the range of thousands to few tens of thousands of transactions per second (with strict serializability). More modern databases can usually scale this number up by introducing additional compute nodes in the system. Some of those systems lose certain consistency guarantees in doing so.
* **Transaction latency** - a typical time needed to complete a single transaction. For most modern database systems with strong consistency guarantees, this is usually, at least, the time necessary to perform two network round trips between different compute nodes (most distributed consensus algorithms require this) plus the time it takes to persist the transaction to a transaction log. In-memory databases usually have latency times significantly smaller than disk based ones.
* **Available interfaces** - or in other words how do you access the database. For a very long time the defacto standard in this area has been SQL. Many modern databases support it, either for reading or both reading and writing the data. Besides SQL, an important aspect is what programming languages can be used to access the database. On top of all this, many developers prefer to write transactional logic using an object oriented approach, where many different ORM frameworks are used to map relational data to objects.
* **Storage type** - whether it is disk based or memory based. This determines how the underlying storage engine of the database stores the data. Both types of databases persist all the data in a persistent storage so that the data is not lost on machine failure or restart. Where they differ is in a way data is organized inside the system memory. In-memory databases store an entire dataset in system memory allowing faster access to it. Contrary to this, disk based databases store an entire dataset on disk and retrieve it to system memory as the data is being accessed. This allows them to cater to datasets much larger than the amount of available system memory. Obviously, this comes at a cost in performance. In-memory databases regularly achieve orders of magnitude higher throughput levels accompanied with reduced latency.
* **Sharding support** - whether the data can be split into multiple compute nodes allowing for extremely large databases.
* **Scale-out support for reads and/or writes** - indicates whether read and read-write requests can be satisfied from more than one node at a time. This is closely related to the sharding support, where different shards can serve read and read-write requests. Even without sharding, databases can offer some level of scale-out capabilities, for example, for read requests. Many databases that offer sharding with scalability usually sacrifice some aspects of consistency or performance (when accessing multiple shards in a single transaction), though there are some that claim to achieve all in most use cases.
* **Cost** - relates to the total cost required to achieve certain level of performance with a given dataset size.
* **Index support** - what kinds of indexes are supported, what is the cost of maintaining them, do they offer the same consistency levels as the rest of the database.

The current market covers many of these aspects well. However, there isn't a single choice that covers all of them. Some databases sacrifice consistency (in general or in some use cases) in order to allow (almost) infinite scalability. Others provide high levels of consistency, but either offer no write scalability or offer it at the high cost (requiring tens to hundreds of nodes to achieve transaction throughput in the order of hundred thousand TPS). There appears to be missing a solution that allows for a very high transaction throughput with strong consistency at a relatively low cost.

## What is VeloxDB
VeloxDB is a cross platform database solution, available on Linux as well as Windows operating systems. As is the case with our competitors, VeloxDB tries to excel in certain aspects while making sacrifices in others. We, however, believe that we cover a very unique subset of aforementioned criteria. Following is a list of some of the most important features of VeloxDB:
* **High performance at a relatively low cost** - one million transactions per second is achievable with a lower cost compared to the competition (including hardware, licensing, support and maintenance costs).
* **Strong consistency** - provides strict serializability guarantees for read and read-write transactions, with optional non-strict serializability for reads. Referential integrity guarantees are available as well.
* **High availability** - up to four write replicas with synchronous replication are supported. VeloxDB, however, does not support sharding and write scale-out capabilities. This essentially means that a single write replica is designated as the primary at any point in time and it is the only replica that can accept write requests.
* **Scale-out for reads** - you can introduce as many read-only replicas as needed.
* **Schema-ful database** - VeloxDB requires a user defined data model. Model is strictly enforced in the database (similar to relational databases). Whenever you perform a model update (schema change) data needs to be transformed to accommodate the new model.
* **Object oriented** - you define your data model and business logic in a .NET language. There are no special limitations on how you write your transactions (e.g. needing to know your read/write sets in advance, or not being allowed to access multiple values/documents in a single transaction).
* **In-memory** - entire data set needs to fit into the system memory. With decreasing costs of RAM, more and more use cases fit this requirement. However, you do need to take into account the existing dataset size as well as the expected future growth. Following chapters offer more details on how to estimate memory requirements of a specific data model.
* **Specialized inverse reference index** - allows for very fast navigation of inverse references. Usually, in a relational database, when you have a foreign key, you create an index on that key so that you can quickly navigate the key in a backwards direction (and to improve speed of cascade delete operations). VeloxDB maintains these reverse references in a data structure that is specifically optimized for that use case (unlike the indexes in most other databases). This makes VeloxDB a great fit for situations where the transactional logic is complex, where read and write sets need to be discovered by moving through a large number of references (e.g. graph algorithms).
* **Merging of data and logic** - in VeloxDB business logic is hosted inside the database and executes in the same process as the database itself. Keeping the logic as close as possible to the data allows for an extremely low latency for many kinds of operations. For example, it is possible to traverse an object graph of one million objects in less than a second, without knowing in advance all the objects that need to be read from the database. Traditional ORM frameworks would require a large number of round trips to the database to discover the entire read set. Additional advantage of this approach is that you do not need to worry about what object properties are needed for a particular operation, whereas, in existing ORM frameworks you might be forced to write different filters for different operations in order to reduce the amount of unneeded data being pulled from the database.

Many features have not been mentioned here but will be covered throughout the remaining chapters of this guide.

VeloxDB comes in two editions. The enterprise edition, which is not free, comes with the complete set of features. The community license, which is free, lacks support for database clusters. There are no other limitations to the standard license.

## Performance
In many systems, it is not uncommon for the performance bottlenecks to arise at the database layer. The reason for this being that the database contains the shared state of the system, so naturally it represents a high contention point. Some systems try to reduce this contention by splitting this state into multiple relatively independent parts (microservice architectures). Doing so sacrifices many of the benefits of the logically centralized database, while still resulting in an uneven distribution of load and database bottlenecks.

Given all this, it is essential to have a solid understanding of the performance characteristics of a given database in a wide array of scenarios. For now, we provide performance measurements for the most typical on-line transaction processing (OLTP) use cases. We plan to introduce additional performance benchmarks in the near future to cover wider range of scenarios. These include [Linked Data Benchmark Council's Social Network Benchmark](https://ldbcouncil.org/benchmarks/snb/) as well as [TPC-E](https://www.tpc.org/tpce/) benchmark.

 ### CRUD Performance
CRUD performance is critical for most OLTP systems. Usually the dominant operations in these kinds of systems are represented by small transactions executing insertion, update, deletion and retrieval of one to few entities.

Following is the definition of the class used in this example. This class has no affected indexes other than the primary key index which is mandatory in VeloxDB. Do not worry at this point if some aspects of the code are not fully clear.

>[!NOTE]
>Source code for this performance benchmark is available in VeloxDB repository at location Samples/Performance/CRUDPerfSample.

[!code-csharp[Main](../../../Samples/Performance/CRUDPerfSample/Server/Vehicle.cs#Vehicle)]

The test executes insertion/update/deletion and read-update (reading of an entity followed by the update of another entity) of a small number of objects (per transaction) with the concurrency level set to 8000. Concurrency level represents the number of concurrent independent requests issued against the database.

All measurements were done on a standard hardware, specifically AWS c6a.8xlarge instances. Two cluster configurations were benchmarked, a single node setup as well as a high availability (HA) cluster composed of two write replicas. Configuration with two write replicas produces lower performance since the data is synchronously replicated from the primary to the standby replica (using a consensus protocol). VeloxDB cluster support is explained in chapter [Database Cluster](database_cluster.md).

>[!NOTE]
>Network bandwidth required to achieve transaction throughput shown here is approximately 2 Gbps per machine. Additionally, two separate EBS gp3 storage devices were assigned to each machine, one for log files, one for snapshot files.

All measurements are given in millions of transactions per second [MT/s]. Keep in mind that the results shown here where measured internally by the VeloxDB team. Feel free to try it out yourself.

| Number of objects<br>per transaction| Insert \[MT/s\] | Update \[MT/s\] | Delete \[MT/s\] | Read-Update \[MT/s\] |
|-------------------------------------|--------|--------|--------|-------------|
| 1                                   | 2.1    | 2.7    | 2.6    | 2.6         |
| 2                                   | 1.8    | 1.9    | 1.5    | 2.2         |
| 4                                   | 1.4    | 1.5    | 1.0    | 1.6         |

<center><b>Table</b> - Write performance, no secondary indexes, single node</center>
<br/>

| Number of objects<br>per transaction| Insert \[MT/s\] | Update \[MT/s\] | Delete \[MT/s\] | Read-Update \[MT/s\] |
|-------------------------------------|--------|--------|--------|-------------|
| 1                                   | 1.8    | 2.5    | 2.4    | 2.3         |
| 2                                   | 1.5    | 1.7    | 1.5    | 1.9         |
| 4                                   | 0.7    | 0.9    | 0.9    | 0.7         |

<center><b>Table</b> - Write performance, no secondary indexes, HA cluster</center>
<br/>

Notice how the number of processed transactions per second drops when increasing the number of affected objects per transaction, but the overall number of processed objects increases. This demonstrates the benefits of grouping multiple operations together into larger transactions (when possible).

As already stated, the previous example does not affect any indexes, except for the mandatory primary key index. Lets take a look at what happens when we start modifying properties that are included in additional indexes. In VeloxDB every reference from one class to another is automatically indexed so that you can quickly navigate the reference in the reverse direction. In the following example, the Ride class references the Vehicle class. This means that every time we modify that reference, inverse reference index must be updated as well.

[!code-csharp[Main](../../../Samples/Performance/CRUDPerfSample/Server/Ride.cs#Ride)]

| Number of objects<br>per transaction| Insert \[MT/s\]| Update \[MT/s\]| Delete \[MT/s\]|
|-------------------------------------|--------|--------|--------|
| 1                                   | 2.0    | 2.0    | 2.8    |
| 2                                   | 1.8    | 1.4    | 1.8    |
| 4                                   | 1.1    | 0.8    | 1.0    |

<center><b>Table</b> - Write performance, inverse reference index, single node</center>
<br/>

| Number of objects<br>per transaction| Insert \[MT/s\]| Update \[MT/s\]| Delete \[MT/s\]|
|-------------------------------------|--------|--------|--------|
| 1                                   | 1.7    | 1.8    | 2.5    |
| 2                                   | 1.6    | 1.1    | 1.6    |
| 4                                   | 0.7    | 0.7    | 1.0    |

<center><b>Table</b> - Write performance, inverse reference index, HA cluster</center>
<br/>

As you can see, affecting a single additional index has a negative impact on performance. Detailed analyses of pros and cons of using different kinds of indexes in VeloxDB are given in their respective chapters.

VeloxDB strictly distinguishes between read-only transactions and read-write transactions. The following example demonstrates pure read performance, with three use cases. The first reads the Ride object by its id, the second reads the Ride object and then follows the reference to the associated Vehicle object and the third case reads from the inverse reference index (finds all Ride objects associated with a given Vehicle object).

| Number of objects<br>per transaction| Read by Id \[MT/s\]| Read by Id and follow reference \[MT/s\]|Read by Id and follow inverse references|
|-------------------------------------|-----------|-----------------------|-----------------------|
| 1                                   | 7.8       | 7.1                   | 6.2                   |
| 2                                   | 5.0       | 4.9                   | 4.3                   |
| 4                                   | 2.7       | 2.4                   | 2.3                   |

<center><b>Table</b> - Read performance, single node</center>
<br/>

No HA cluster results are presented for read performance since it is not affected by the existence of a cluster.

These tests by no means give a detailed analysis of performance characteristics of VeloxDB, but are meant to be used as a rough guide when deciding whether VeloxDB is a good fit for your needs. Different data models, logic complexity, contention patterns and many other things can significantly affect the performance, so be sure to measure your specific use cases.

## Known Limitations
This section identifies some well known limitations of VeloxDB. Many of these limitations have been "designed" into the engine from the beginning as a compromise that needed to be made in order to achieve certain goals. Other have been identified during usage. Keep in mind that as we keep working on VeloxDB, this list might change. We constantly strive to bring improvements to VeloxDB.
* **Data set size** - mentioned earlier, this represents a hard limit that can't be easily circumvented, meaning if the data set is larger (or is expected to become larger) than the amount of available system memory, VeloxDB is probably not the best choice. See the next section for a possible mitigation of this limitation.
* **Limited transaction scope** - transactions cannot be controlled from the client API (neither started nor committed or rolled back). This forces the user to execute all transactional logic inside the database hosted operations. While this might be viewed as a limitation, this allows for very complex business operations to be implemented where an entire object graphs needed for the execution of the operation are discovered on the fly. In these use cases, typical systems would spend extreme amounts of CPU cycles executing network round trips between the application server and the database. Users can, however, extract any needed data from the database for an out-of-transaction processing.
* **Blocking model update** - updates of the data model (object model) hosted inside the database are blocking operations, meaning, user transactions are blocked from executing until the model update is complete. Most common model update operations (e.g. insertion of new classes or insertion of properties into existing classes) usually execute very quickly. See [Model and API Update](model_api_update.md) chapter for details on the specific model update operations and their expected performance implications.
* **Lack of scale-out and sharding** - You need to plan your data set size and expected growth carefully.
* **Unbounded transaction execution time** - transactions in VeloxDB execute in parallel, isolated from each other with serializable isolation level. This isolation level guarantees that transactions executing in parallel are executed in a way that is equivalent to some serialized execution order. This is achieved with conflict detection mechanism. If two transactions are in conflict with one another, they are not allowed to execute in parallel and one of them is rolled back. The choice which transaction is rolled back, currently, is not user controllable. This might lead to a single (usually long) transaction never being able to commit due to constant conflicts with other (usually short) transactions. We do have plans to introduce the concept of transaction priority, allowing higher priority transactions (the ones being retried because of a conflict) to execute with higher priority, which will prevent them from being rolled back by transactions with lower priority.

## What's to come
We want to include many additional features in VeloxDB. Here is just an overview of some of the most important features from that list:
* **SQL Select** - possibility to read data from the database using the SQL Select statement. There are currently no plans to allow data (and schema) modifications using SQL.
* **Disk based classes** - classes stored not in system memory (as regular VeloxDB classes) but on a persistent storage. These classes would offer high insert and read performance (similar to existing in-memory classes) but with a limited update/delete performance. Many systems produce large quantities of data, which, once inserted into the database, are rarely modified. An example of this might be a chat application where user messages, once posted, are almost never modified. Disk based classes would be ideal for these scenarios, with support for arbitrary number of indexes and scale-out capabilities, allowing for practically unlimited amounts of data.
* **Time-series classes** - as a special kind of disk based classes, time series classes represent, as the name implies, a series of timestamped values, usually of some decimal type. With built-in lossless and lossy compression options, users would be able to collect large amounts of values in an optimized manner. Together with disk based data, this feature significantly expands the scope to which VeloxDB can be applied, as it no longer limit the size of an entire data set, provided that the data stored on disk is rarely modified.
* **BTree index** - this is a common index type in many databases that optimizes range queries (which is currently missing from VeloxDB).
* **Spatial index** - allowing for multidimensional range queries, it is an ideal index for storing and searching geo-spatial data.
* **Full text search** - well known index type, optimizes text pattern search queries.
* **Many other client APIs** - at the moment, only .NET client API is supported, but many more are coming, including JavaScript, Python, Java and others.

## Conclusion
The main purpose of this introductory chapter was to give you, a system designer, the necessary knowledge to make an educated decision on whether VeloxDB fits your specific needs. Choosing the underlying database technology for a complex system might be the most important decision you have to make. Many traps are laid out in front of you either by the database vendors or by the complex nature of concurrent systems. Pay special attention to the consistency guarantees, is it OK to forfeit some of these in order to achieve better scalability. Bear in mind that the business logic might get more and more complex as the system ages, and writing complex concurrent systems without proper isolation might lead to data loss, data corruption and difficult to find bugs. The main selling point of VeloxDB is its ability to achieve high performance at low cost with strong consistency guarantees, all this with very simple object oriented interface. There are ready-to-use Samples in the [VeloxDB GitHub repository](https://github.com/VeloxDB/VeloxDB), or you can create some by for yourself. Its very easy to [get started](simple_example.md).
