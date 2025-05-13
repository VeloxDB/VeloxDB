# VeloxDB's Transaction Model and Consistency Guarantees
## Introduction

I'd like to take some time to explain VeloxDB's transaction model and consistency guarantees. This is important because many databases cut corners in this area, and it is one of the primary strengths of VeloxDB.

VeloxDB is a fast, in-memory (but with persistance) object-oriented (OO) database. It provides exceptional performance while offering an object-oriented interface. In VeloxDB, everything is done using C#, from model definition to data manipulation. The experience is similar to using a traditional ORM. You define the model with C# classes and access it with C# code. Database access is organized through database operations—C# methods executed within the database itself, similar to stored procedures in SQL databases. If you'd like to learn more about VeloxDB, please refer to my other article, [Introducing VeloxDB](https://dev.to/defufna/introducing-veloxdb-26j1). For now, I'll provide an example of a VeloxDB database operation:


```cs
[DbAPIOperation]
public long CreateBook(ObjectModel om, string title, string author)
{
	Book newBook = om.CreateObject<Book>();
	newBook.Title = title;
	newBook.Author = author;
	return newBook.Id;
}
```

Here is a simple database operation that creates a book using the provided parameters. It is a standard C# method that accepts an ObjectModel as the first argument, while the remaining arguments can be anything the user desires. The `ObjectModel` is a class that grants access to the database itself. By utilizing the object model, database operations can read data from the database and create, update, or delete objects within it.

## The ACID Transaction Model
Each database operation represents a single [ACID](https://en.wikipedia.org/wiki/ACID) transaction. Let's break down each component of ACID and discuss what it means in the context of VeloxDB.

Atomicity - Each database operation is executed atomically. This means that either all changes are applied to the database or none; there are no partial executions. If the method executes successfully, the changes are applied. If it fails due to an exception being thrown or the user manually calling rollback, all changes made are rolled back, leaving the database as if the operation never occurred.

Consistency - VeloxDB ensures that each transaction leaves the database in a consistent state. VeloxDB validates references and user-specified model constraints, such as reference cardinality, nullability, and hash index uniqueness guarantees, among others. Transactions that violate these constraints will not be committed.

Isolation - Each transaction is isolated from other transactions. In other words, each transaction appears as if it is the only one running on the system. In reality, various databases implement isolation differently, and not all isolation levels provide full isolation. VeloxDB offers strict serializability, the gold standard of transaction isolation. We will revisit this topic later in the article.

Durability - Transactions are persisted on the hard drive. Once a transaction is complete, all its data is safely stored on the drive.

These four properties comprise the "secret sauce" of a database, making them incredibly convenient to work with.

## The Importance of Transactions

Many bugs encountered in everyday work could be resolved by utilizing transactions. For instance, if every try/catch block in C# were atomic, it would significantly reduce the number of bugs created. Writing multithreaded software would also be much easier with transactions. Although there have been attempts to integrate transactions into common programming languages, such as software transactional memory and transactional memory, transactions are typically found only in databases for now.

A clear example of how transactions simplify work can be seen in the standard architecture of web applications. These applications inherently exhibit a high degree of concurrency, as countless users—potentially in the thousands or even millions—simultaneously access and modify shared data. Interestingly, the average web developer seldom contemplates concurrency. This lack of focus on concurrency is actually beneficial, as addressing concurrent issues for each new feature added to a web application would be unsustainable. 

In the typical web application, the entirety of the state resides in a database, and all operations are carried out as transactions. This arrangement entrusts the database with the responsibility of managing concurrency, allowing developers to concentrate their efforts on crafting the business logic.

Concurrency is the challenging aspect; if tasks were executed in a purely serial fashion instead of concurrently, the situation would be markedly more straightforward. However, modern systems have multiple cores, and it would be inefficient for a database to use only one core. If multiple transactions are available, it is logical to use multiple cores for processing them.

Even in cases where only one core is available, transactions frequently encounter delays while waiting on disk or network resources. During these periods, the lone core could be more effectively engaged in processing other transactions. As a result, transactions are executed concurrently for the sake of performance.

Concurrent execution of transactions, however, necessitates the implementation of isolation to ensure that transactions do not conflict with one another, thereby jeopardizing the integrity of the database. The primary function of isolation is to segregate transactions from each other. In an ideal scenario, each transaction would be as isolated as if it were the sole transaction operating within the database. This degree of isolation is referred to as serializable isolation.

Nevertheless, serializable isolation is not the highest possible isolation level. Its definition lacks a time component, and it allows the database to rearrange transactions as it sees fit. By introducing an additional criterion - namely, that if transaction T2 is executed after transaction T1 completes execution, T2 must see the results of T1 - we attain the highest isolation level: strict serializable isolation. This is the isolation level employed by VeloxDB.

With strict serializable isolation, your system behaves as if it is executing each transaction sequentially. As a developer, you don't need to worry about any anomalies and can simply focus on writing your business logic. This isolation level should be the default, as specified in the SQL 92 standard. However, many databases opt for weaker isolation levels for performance reasons.

The problem with weaker isolation levels is that they can exhibit various anomalies in a concurrent environment. These anomalies include [dirty reads](https://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Dirty_reads), [non-repeatable reads](https://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Non-repeatable_reads), [phantom reads](https://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Phantom_reads), [lost updates](https://en.wikipedia.org/wiki/Write%E2%80%93write_conflict), and [write skew](https://www.cockroachlabs.com/blog/what-write-skew-looks-like/). These issues are not just theoretical - they can and do happen on real systems running in production.

For example, in 2017, researchers from Stanford analyzed 12 popular eCommerce solutions for exploits based on weak isolation anomalies. They identified 22 critical attacks that allowed attackers to corrupt store inventory, overspend gift cards, and steal inventory ([ACIDRain: Concurrency-Related Attacks on Database-Backed Web Applications](http://www.bailis.org/papers/acidrain-sigmod2017.pdf)). This is a serious concern, which is why VeloxDB uses strict serialization as its default isolation level.

It's worth noting that all other isolation levels exhibit various anomalies, which you have to watch for and design for. As your code base and the number of people working on it grow, this becomes increasingly difficult to do. Therefore, I strongly recommend that you use strict serializable isolation as your default isolation level.

I'm not arguing that lower isolation levels shouldn't exist. It's perfectly acceptable to lower the isolation in specific performance bottlenecks after you have thoroughly analyzed the consequences. However, weaker isolation levels should not be the default. By defaulting to strict serializable isolation, you'll avoid many potential issues and make your system more secure and reliable.

## VeloxDB Transaction implementation 

Although VeloxDB ensures transactions are executed as if they were sequential, the database engine runs them in parallel for better performance. In order to execute transactions in parallel without compromising on serializability isolation, VeloxDB relies on [multiversion concurrency control (MVCC)](https://en.wikipedia.org/wiki/Multiversion_concurrency_control). In this section, we'll dive into how MVCC enables VeloxDB to achieve this

The core principle of MVCC is that updates are processed by inserting new objects with updated versions. Consequently, an MVCC database can store multiple versions of a single object, which is why it is called "multiversion." 

Let's consider a sequence of transactions using our book model as an example:

```
T1: AddBook("The Great Gatsby", "F. Scott Fitzegarld")
T2: AddBook("To Kill a Mockingbird", "Harper Lee")
T3: AddBook("One Hundred Years of Solitude", "Gabriel Garcia Marquez")
T4: UpdateBookAuthor(1, "F. Scott Fitzegarld")
```

The resulting Book table would appear as follows:

| Id | Title                  | Author                        | Version |
|----|------------------------|-------------------------------|---------|
| 1  | The Great Gatsby       | F. Scott Fitzegarld           | 1       |
| 1  | The Great Gatsby       | F. Scott Fitzgerald           | 4       |
| 2  | To Kill a Mockingbird  | Harper Lee                    | 2       |
| 3  | One Hundred Years of Solitude | Gabriel Garcia Marquez | 3       |

You may notice that the book with ID 1, "The Great Gatsby," appears twice in the table with different versions. This is because the book was updated to correct a misspelling of the author's surname. The version number represents the ID of the transaction that changed or inserted the record. Transaction IDs are assigned upon commit and are incremental.

Since objects are immutable, it is trivial to provide a snapshot of the database at a particular version. For example, if a user opened a read transaction at version 3 before "The Great Gatsby" was updated, to provide the user with the snapshot, we would simply filter the table with the criteria "Version ≤ 3." The read transaction's snapshot would remain unchanged even after the update since the filter excludes any newer versions of the object. The version at which the transaction sees its snapshot of the database is called the "read version". The read version is the version the transaction observed when it was started.

## Conflict resolution

It is evident that this method offers simple read transaction isolation. However, let us now explore read-write transactions. In the absence of conflicts, MVCC essentially functions in the same way for multiple read-write transactions as it does for read transactions. For example, suppose transactions T1, T2, and T3 from the above sequence were all running concurrently. This means they all began with read version 0, and their snapshot was an empty database. Each transaction added a new book, and all of them were able to commit successfully without any conflicts.

However, conflicts may arise when multiple read-write transactions try to modify the same object concurrently. To handle such conflicts, VeloxDB employs [optimistic concurrency control (OCC)](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) as a conflict resolution strategy. In optimistic concurrency control, read-write transactions are permitted to proceed without any locks. If a conflict arises with another transaction running concurrently, the transaction is aborted and must be retried. This indicates that two transactions were unable to run in parallel, and therefore one had to be aborted. By retrying the aborted transaction, you essentially serialize them.

Here is a list of situations that can cause conflicts:
* Two transactions modify the same object
* A transaction modifies an object that another transaction has read
* A transaction scans a table that another transaction has inserted a new object into

This list is not exhaustive for simplicity reasons, as there are other situations that can cause transactions to be in conflict. Only transactions whose executions overlap can be in conflict. Transactions that have not run concurrently will never be in conflict.

There is a separate garbage collection process that periodically cleans unnecessary records from memory. Unnecessary records are those that exist at a newer version and have a lower version than the lowest read version of all executing transactions. This means that these records will never be read and can safely be removed from memory.
## Final words

Strict serialization transactions are a powerful tool that, in my opinion, is too easily discarded these days. Many modern databases offer weaker isolation levels, and they often obscure the exact guarantees they provide. Although weaker isolation levels can improve performance and scalability, they introduce problems that are challenging to diagnose and rectify.

In conclusion, we have examined the transaction-handling capabilities of VeloxDB. With its strict serialization approach, combined with a sleek ORM-like interface and cutting-edge performance, VeloxDB is a powerful tool.

For more information, check out these helpful resources:
Website: https://www.vlxdb.com
User Guide: https://vlxdb.com/guide/introduction.html
GitHub: https://github.com/VeloxDB/VeloxDB
Twitter: https://twitter.com/VeloxDB