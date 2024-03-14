---
uid: getting_started
---
# Getting Started
Before we take a deeper dive into different aspects of VeloxDB, now is a good time to briefly cover several important topics essential for further understanding of this guide. Even though most of these topics are covered in more details later, it is beneficiary to obtain at least a basic understanding of them before proceeding further. These topics include data model and APIs, deployment, configuration, administration, client side programming as well as tracing and debugging. We start by creating a simple VeloxDB application.

## Simple Application
The application consists of two parts, building a server side library and building a client application.

### Building a server

In VeloxDB, data model and business logic are defined by a way of user supplied .NET assemblies. If you have any experience with traditional ORM frameworks (e.g. Entity Framework Core) you will find using VeloxDB familiar.

#### Create a new project

#### [.NET CLI](#tab/net-cli)

```sh
dotnet new classlib -o vlxblog
cd vlxblog
```

#### [Visual Studio](#tab/visual-studio)

* Open Visual Studio
* Click **Create a new project**
* Select **Class library** with the **C#** tag and click **Next**
* Enter **vlxblog** for the name and click **Next**
* Select **.NET 7.0** for framework and click **Create**.
---

#### Change project platform to x64
VeloxDB supports only x64 platform. In order to avoid build warnings, it is recommended to target x64 platform.

#### [.NET CLI](#tab/net-cli)

* Open vlxblog.csproj project file in text editor
* Add ```<PlatformTarget>x64</PlatformTarget>``` under ```<PropertyGroup>``` tag

#### [Visual Studio](#tab/visual-studio)

* Open Project 🡒 vlxblog Properties
* Go to Build 🡒 General
* Under **Platform target** select **x64** from dropdown list
* Click File 🡒 Save vlxblog
---

#### Add NuGet package

#### [.NET CLI](#tab/net-cli)

```sh
dotnet add package VeloxDB
```

#### [Visual Studio](#tab/visual-studio)

* Open Project 🡒 Manage NuGet Packages
* Click **Browse** and in Search type in **VeloxDB**
* Click on the VeloxDB package and click on **Install**
---

#### Define the model

Model is defined using regular C# classes. Add a Model.cs file to your project with the following content:

[!code-csharp[Main](../../Samples/GetStarted/VlxBlog/Model.old.cs)]

Let's take a closer look at the classes defined here. Model classes must be declared as abstract, this is also true for all database properties. This allows VeloxDB to dynamically create implementation of classes at runtime.

Model classes must inherit from the <xref:VeloxDB.ObjectInterface.DatabaseObject> class, which defines common properties and methods for all database classes (for example [Id][4] and [Delete][5]).

Classes and properties must be marked with attributes <xref:VeloxDB.ObjectInterface.DatabaseClassAttribute> and <xref:VeloxDB.ObjectInterface.DatabasePropertyAttribute> respectively. Also note <xref:VeloxDB.ObjectInterface.InverseReferencesAttribute> attribute. You use this attribute to define inverse reference properties. In this case post has a reference to blog, using <xref:VeloxDB.ObjectInterface.InverseReferencesAttribute> we declare a property that allows us to easily get all posts of a single blog.

Post.Blog property is marked with <xref:VeloxDB.ObjectInterface.DatabaseReferenceAttribute>, which we use to specify additional information about the reference. The first argument specifies whether the reference can be null. In this case it doesn't make sense to have a post without a blog. The second argument specifies what should a database do when the target object is deleted. In case a blog is deleted, we want all of its posts to be deleted as well, so we set it to cascade delete. The last argument specifies whether the database will track inverse references for this reference. Since we plan to navigate this reference in reverse direction (from blog to post) we set this to true.

#### Create DTOs
We will now define DTO classes, which will be used to transfer data between the client and the server. Create DTO.cs file with the following content:

[!code-csharp[Main](../../Samples/GetStarted/VlxBlog/DTO.cs)]

<!-- For more information about why we use DTOs see [Data Transfer Objects](../tech.md#data-transfer-objects).-->

#### Create ToDTO and FromDTO methods
Copying data between model and DTOs can be cumbersome. For that reason VeloxDB comes with builtin automapper that can save you from typing some boilerplate code. To use automapper make Blog and Post database classes in Model.cs partial. Also add highlighted methods to Blog and Post.

[!code-csharp[Main](../../Samples/GetStarted/VlxBlog/Model.cs?highlight=16-18,33-35)]

Automapper is implemented using [C# Source Generators][6]. The partial methods you created will be implemented by the source generator at compile time. There are two types of methods: To and From methods. To create an automapper method you need to add a partial method to the database class. Currently automapper methods are only supported for database classes. The method needs to start with an appropriate prefix (To/From). It also has to have correct arguments and return type. You can see the specific requirements for each method type in the table below.

| Method Prefix | Description           | Is static | Arguments        | Return type |
|---------------|-----------------------|-----------|------------------|-------------|
| To            | Copies DBO to DTO     | No        | No arguments     | DTO         |
| From          | Creates DBO from DTO  | Yes       | ObjectModel, DTO | DBO         |

For more detailed information see [Automapper](database_apis.md##automapper) section of the guide.

#### Create database API
All business logic is defined in server side methods, we call these methods database operations. By keeping the business logic and the data in the same process, VeloxDB avoids multiple round trips to the Database. Create BlogApi.cs file with the following content:
[!code-csharp[Main](../../Samples/GetStarted/VlxBlog/BlogApi.cs)]

The methods described in this file can be called from the client using VeloxDB.Protocol library. This will be covered in the next section. To define a database API operation you need to create a class marked with the <xref:VeloxDB.Protocol.DbAPIAttribute>. Each method should be decorated with the <xref:VeloxDB.Protocol.DbAPIOperationAttribute>.

<!-- For more information see [VeloxDB Protocol](../tech.md#veloxdb-protocol).-->

The first argument of all Database operations must be of type <xref:VeloxDB.ObjectInterface.ObjectModel>. <xref:VeloxDB.ObjectInterface.ObjectModel> is used to access the data in the database. In this example we use the [GetObject][2] method to read objects, by Id, from the database and [CreateObject][3] to create new objects.

Each operation corresponds to a single transaction inside the database. While an operation is running, changes it makes to the model are not visible to other operations. After an operation completes successfully its changes are committed to the database automatically. If an operation throws an exception all changes are rolled back. If your operation only reads data from the database, you can set <xref:VeloxDB.Protocol.DbAPIOperationAttribute.OperationType> to <xref:VeloxDB.Protocol.DbAPIOperationType.Read>. For more information about transactions see [Transactions](architecture.md#transactions).

#### Running the server

#### [.NET CLI](#tab/net-cli)
```sh
dotnet run
```
#### [Visual Studio](#tab/visual-studio)
* **Debug** 🡒 **Start Without Debugging**
---

You should see an output similar to this:

```accesslog
23-01-01 20:58:32.018 [7C03] [Info] Starting VeloxDB 0.2.2.0
23-01-01 20:58:32.263 [7C03] [Info] Storage engine started.
23-01-01 20:58:32.301 [7C03] [Info] Looking for changes in C:\VeloxDB\vlxblog\bin\x64\Debug\net8.0\
23-01-01 20:58:32.472 [7C03] [Info] User assemblies updated.
23-01-01 20:58:32.478 [7C03] [Info] Initializing persistence to C:\VeloxDB\vlxblog\bin\x64\Debug\net8.0\vlxdata.
23-01-01 20:58:32.532 [7C03] [Info] Persistence configuration updated.
23-01-01 20:58:32.541 [7C03] [Info] Administration endpoint hosted on 127.0.0.1:7569.
23-01-01 20:58:32.542 [7C03] [Info] Execution endpoint hosted on 127.0.0.1:7568.
23-01-01 20:58:32.571 [7C03] [Info] Server successfully started.
```

VeloxDB NuGet package comes bundled with the VeloxDB server. When you run your project, the server starts, loads the assembly and creates the database if it doesn't already exist. The database files are placed in project's output directory in vlxdata subdirectory. This is the development deployment. For the recommended production deployment please see [Downloading and configuring the database](#downloading-and-configuring-the-database).

### Building a client

#### Create a new console project
Now that we have a working server, it's time to build a client.

#### [.NET CLI](#tab/net-cli)

```sh
dotnet new console -o vlxclient
cd vlxclient
```

#### [Visual Studio](#tab/visual-studio)

* Click **File** 🡒 **Add** 🡒 **New project...**
* Select **Console App** with the **C#** tag and click **Next**
* Enter **vlxclient** for the name and click **Next**
* Select **.NET 6.0** for framework and click **Create**.
---

#### Change project platform to x64

#### [.NET CLI](#tab/net-cli)

* Open vlxclient.csproj project file in text editor
* Add ```<PlatformTarget>x64</PlatformTarget>``` under ```<PropertyGroup>``` tag

#### [Visual Studio](#tab/visual-studio)

* Open **Project** 🡒 **vlxclient Properties**
* Go to **Build** 🡒 **General**
* Under **Platform target** select **x64** from dropdown list
* Click **File** 🡒 **Save vlxclient**
---

#### Add NuGet package

#### [.NET CLI](#tab/net-cli)

```sh
dotnet add package VeloxDB.Protocol
```

#### [Visual Studio](#tab/visual-studio)

* Open Project 🡒 Manage NuGet Packages
* Click **Browse** and in Search type in **VeloxDB.Protocol**
* Click on the VeloxDB.Protocol package and click on **Install**
---

VeloxDB.Protocol package is used for calling public database APIs.

#### Reference the vlxblog project

In order to avoid having to reimplement DTOs on the client side, we will reference the previously created vlxblog project.
#### [.NET CLI](#tab/net-cli)

```sh
dotnet add reference ../VlxBlog/vlxblog.csproj
```

#### [Visual Studio](#tab/visual-studio)

* Click on **vlxclient** project to select it
* Go to **Project** 🡒 **Add project reference...**
* Check the checkbox next to **vlxblog** project
* Click on **Ok** button
---

&nbsp;
&nbsp;

>[!NOTE]
>This example is built with simplicity in mind, in real applications we suggest you place your DTOs in separate class library
>that you can share between the client and the server code.

#### Define public server interface

Add an IBlogApi.cs file to your project with the following content:

[!code-csharp[Main](../../Samples/GetStarted/VlxClient/IBlogApi.cs)]

The interface must be marked with the <xref:VeloxDB.Protocol.DbAPIAttribute>. Note the name parameter, as it specifies the name of the database API we target with this interface. Default name for a database API is the full .NET name of the API class. Then we specify all operations we have defined on the server. Note that operations omit ObjectModel argument here. ObjectModel is injected server side and is not needed (or available) client side. Each operation is marked with the <xref:VeloxDB.Protocol.DbAPIOperationAttribute>.

#### Create main method

Replace the content of Program.cs with the following:

[!code-csharp[Main](../../Samples/GetStarted/VlxClient/Program.cs)]

We first create an instance of <xref:VeloxDB.Client.ConnectionStringParams>. This is a helper class that will help us create the connection string. We add our server's address to it (VeloxDB's default port is 7568). Then we use <xref:VeloxDB.Client.ConnectionFactory> to get the connection to the server. Once we have the connection, calling database operations is trivial, we just call appropriate methods.

#### Run the app

Make sure you have the server started and run the client.
#### [.NET CLI](#tab/net-cli)
```sh
dotnet run
```
#### [Visual Studio](#tab/visual-studio)
* **Debug** 🡒 **Start Without Debugging**
---

You should see the following output:

```txt
Blog retrieved from server
Adding post success: True
Deleting blog success: True
```

Congartulations! You have just written your first VeloxDB app.

## Database Model and APIs
Lets take another look at the application we just created. The first step required is to define the data model, represented by a set of .NET classes. VeloxDB is an object database, meaning that the model defined using these classes is the exact model stored in the database. Unlike typical ORM frameworks that need to map object models to relational schemas, VeloxDB performs no such transformation, what you see is what you get. VeloxDB, however, does not store .NET objects internally, it uses an internal data representation which is optimized for other operations a database needs to perform (such as transaction isolation, replication and persistence). Detailed information on how to define database models is provided in chapter [Data Model](data_model.md).

After the data model is defined, VeloxDB requires that the data is manipulated using .NET code (in the future, there will be a possibility to perform read operations using SQL Select statement). This is done in the form of database APIs, a set of publicly exposed APIs that clients can consume. A single database API represents a set of logically grouped operations that transactionally manipulate the data.

>[!NOTE]
>Logical grouping of database operations into database APIs is just a recommendation. The way you group your operations is completely up to you. You can store all operations in a single API or split them in as many APIs as needed. The APIs themselves can reside in a single .NET assembly or multiple assemblies. There are no specific limitations imposed by the database itself.

>[!NOTE]
>Term Database API should not be confused with the term Web API (or REST API). Database APIs you define in VeloxDB are not based on the same technology (do not use HTTP) and use an internal protocol.

The way you define an API is by creating a .NET class that is decorated with the DbAPIAttribute. Operations inside the API class are represented by the class methods decorated with the DbAPIOperationAttribute. Each database operation has to have a first parameter of type ObjectModel. The remaining parameters represent input values for the operation (provided by the client). The operations may not contain out/ref parameters and can only return a single result as a return value of the method. The actual model classes may not be used as parameters of the operations. This would introduce tight coupling between public APIs and internal data representation so VeloxDB strictly forbids it. If you want to transfer entire entities from the client to the server (or vice versa), you should define separate classes from the ones defined in the data model. These classes are usually called DTOs (Data Transfer Objects) since their sole purpose is to transfer data between the client and the server. Tools that automatically map DTOs to model classes and vice versa are available out of the box as part of the VeloxDB to reduce the amount of boiler plate code. Auto-mapping capabilities were briefly demonstrated in the sample application and are covered in mode details in chapter [Database APIs](database_apis.md##automapper).

Each operation can either be a Read operation or a ReadWrite operation (ReadWrite is the default). GetBlog operation in the sample application is an example of a read operation. Read operations, as the name implies, are limited to only reading the data from the database. Attempting to modify the data in any way will result in an exception being thrown.

The major difference between VeloxDB and most other databases is that VeloxDB requires for the application logic (database APIs) to be deployed to the database itself. Keeping the logic close to the data allows for extremely low latency when executing actions against the database. This is especially true for workloads that do not know an entire affected dataset in advance (the dataset is  discovered during the execution of the application logic itself). Databases that offer these capabilities (e.g. stored procedures in relational databases) do not offer object oriented interface for accessing the data and still execute at significantly higher latencies than what VeloxDB can achieve.

>[!NOTE]
>You might think that there is nothing stopping you from creating two database operations, one reading the data for outside processing and one for writing back the results. However, transaction scope in VeloxDB is tied to a single execution of a database operation, meaning you would read the data in one transaction and then write the results in another transaction, thus loosing  transaction isolation provided by the database. VeloxDB transactions are covered in chapter [Architecture](architecture.md##transactions).

## Downloading and Configuring the Database
VeloxDB requires Microsoft [.NET 7](https://dotnet.microsoft.com/en-us/download/dotnet/7.0) to run. After installing .NET, you can download the free version of [VeloxDB](https://www.vlxdb.com/download.html) binaries or download the source code from our [GitHub repository](https://github.com/VeloxDB/VeloxDB) and compile it yourself. The free version includes all VeloxDB functionalities except cluster support (ability to create a database cluster). After downloading, unpack the archive file to a desired directory. There are two configuration files in the unpacked directory, vlxdbcfg.json and config.cluster.json. You can run the server with the default values in these files, however the server will only listen on the localhost (127.0.0.1) address and wont be visible over the network.

config.cluster.json stores the configuration of the VeloxDB cluster. For now we are only going to use a single node deployment. For cluster deployments see [Database Cluster](database_cluster.md) chapter. You can replace the localhost address here with the host name (or IP address) of your server. You can also modify the default port values (if needed). File vlxdbcfg.json contains configuration parameters of the database server grouped into several sections. Some of the parameters represent file system paths. When specifying file system paths, you can use any of the following templates to point to well known locations:

| Template                | Windows                         | Linux                           |
|-------------------------|---------------------------------|---------------------------------|
| ${ApplicationData}      | %APPDATA%                       | $HOME/.config                   |
| ${LocalApplicationData} | %LOCALAPPDATA%                  | $HOME/.local/share/             |
| ${UserProfile}          | %USERPROFILE%                   | $HOME                           |
| ${Base}                 | Database installtion directory  | Database installation directory |
| ${Temp}                 | Current user's temporary folder | Current user's temporary folder |

Following table summarizes available configuration parameters:

| Parameter                                          | Description                                                                                                                                                                                                                                                                                                        | Default value                                                      |
|----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| ExecutionEndpoint/<br>BacklogSize                  | TCP backlog queue size. VeloxDB client APIs use persistent connections to the database server so this number can remain low.                                                                                                                                                                                       | 20                                                                 |
| ExecutionEndpoint/<br>MaxOpenConnCount             | Maximum number of open client connections.                                                                                                                                                                                                                                                                         | 10                                                                 |
| ExecutionEndpoint/<br>BufferPoolSize               | Amount of pooled memory (in bytes) per each client connection. VeloxDB protocol requires memory to serve client requests. This memory is used to accept input data as well as to send response data. Increase this value if you regularly send or receive large amounts of data.                                  | 8MB                                                              |
| ExecutionEndpoint/<br>InactivityInterval           | If no requests have been executed by the connection during this time (in seconds), VeloxDB sends a keep-alive message to the client to verify that the connection is still alive. This is a decimal value.                                                                                                         | 2 sec                                                              |
| ExecutionEndpoint/<br>InactivityTimeout            | Amount of time (in seconds) a client has to respond to a keep-alive message. If no response is received in this time, connection is closed by the server.                                                                                                                                                          | 1 sec                                                              |
| ExecutionEndpoint/<br>MaxQueuedChunkCount          | Determines maximum amount of unprocessed client request data that can be queued by the server before server stops accepting more data. This number needs to be multiplied by 64KB. Increasing this number might lead to faster processing of very large requests but will increase the memory usage of the server. | 64                                                                 |
| Database/<br>SystemDatabasePath                    | Path to a directory where the database server will store system database files. System database is an internal database where the server stores its own data.                                                                                                                                                     | ${LocalApplicationData}<br>/vlxdb/data                             |
| Logging/<br>Path                                   | Path to a directory where the trace files will be stored.                                                                                                                                                                                                                                                           | ${LocalApplicationData}<br>/vlxdb/log                              |
| Logging/<br>Level                                  | Initial trace level. Possible values include None, Error, Warning, Info, Debug, Verbose.                                                                                                                                                                                                                 | Info                                                               |
| Logging/<br>UserLevel                                  | Initial user trace level. Possible values include None, Error, Warning, Info, Debug, Verbose.                                                                                                                                                                                                                 | None                                                               |
| Replication/<br>ThisNodeName                       | Name of the node. When deploying the node inside a database cluster, this name should be the name of one of the nodes available inside cluster configuration file. Cluster configuration is covered in chapter [Database Cluster](database_cluster.md##configuration).                                                                                                                         | Node                                                               |
| Replication/<br>ClusterConfigFile                  | Path to a file containing the configuration of the database cluster. This value is not needed if the database cluster is not used.                                                                                                                                                                                  | ${Base}/<br>config.cluster.json                                    |
| Replication/<br>PrimaryWorkerCount                 | Number of worker threads dedicated to sending the replication data to other nodes in the cluster (per replica).                                                                                                                                                                                                 | 4                                                                  |
| Replication/<br>StandbyWorkerCount                 | Number of worker threads dedicated for receiving replicated data from other nodes in the cluster.                                                                                                                                                                                                                  | 0 (Means proportional<br>to the number of<br>available CPU cores). |
| Replication/<br>UseSeparate<br>ConnectionPerWorker | Indicates whether each primary worker should use its own separate TCP connection to replicate data.                                                                                                                                                                                                                | true                                                               |

You do not need to edit any of these parameters. The default values were chosen to fit most needs. However, you might want to configure directory paths. To start the VeloxDB database server, run the following command:
```sh
./vlxdbsrv
```

Configuration can be loaded from more than one vlxdbcfg.json file. Each successive config file overrides the previously loaded values. Following locations are probed for config files (in this order):
* Database Installation directory
* %PROGRAMDATA%\vlxdb (Windows) or /etc/vlxdb (Linux)
* %APPDATA%\vlxdb (Windows) or $HOME/.config/vlxdb (Linux)
* Command line argument

The last config file can be provided as a command line argument when starting the server. Following example illustrates this:
```sh
./vlxdbsrv --config path/vlxdbcfg.json
```

Since VeloxDB only allows a single database per running server instance, this allows you to run multiple server instances from a single installation directory.

## Client Tool
Once a database server has been configured and started you need to administer the database. For this purpose vlx client tool is used. This tool is a command line application which can be used in one of two modes, interactive or direct mode. You run direct mode the following way:

```sh
./vlx command [parameters]
```

This is the typical way of using the tool, which can also be used to automate many aspects of configuration and administration. Interactive mode, on the other hand, once entered, keeps the application running, allowing you to execute multiple commands manually. Entering interactive mode is accomplished by running the following command:

```sh
./vlx
```
Interactive mode has additional sub-modes that are used to administer some specific areas of the database. For example, if you want to configure database persistence parameters, you need to enter persistence configuration mode by executing:

```sh
persist-config
```
Once in a sub-mode, all the changes are accumulated locally and need to be applied to the database before exiting the sub-mode. This is done by executing the following command:
```sh
apply
```

When finished with a given sub-mode, execute
```sh
exit
```
to exit the sub-mode. This command will exit the application if no sub-mode is active.

Let's see how you can display a list of all available commands:

#### [Direct](#tab/net-cli)
```sh
./vlx help
```

#### [Interactive](#tab/visual-studio)
```sh
help
```
---
&nbsp;
&nbsp;

You can also show help for a single command together with a detailed description of all available parameters. Lets show help for the status command:

#### [Direct](#tab/net-cli)
```sh
./vlx status --help
```

#### [Interactive](#tab/visual-studio)
```sh
status --help
```
---
&nbsp;
&nbsp;

>[!NOTE]
>Most parameters, besides having u full name, also have a short name. Full name is specified by using double hyphens while short name is specified with a single hyphen. For example bind parameter can either be specified with --bind or -b.

Vlx tool works by connecting to the database. This process is called binding. To bind to a database you need to provide the endpoint (hostname:port or address:port) of one or more nodes from the cluster. If no address is provided, vlx will try to bind to a database running on the local machine (localhost) and default VeloxDB administration port (7569). Providing the endpoint for more than one node is useful if one or more nodes in the cluster are unavailable when the command is executed. In interactive mode, binding is established only once, before running any other commands, while in direct mode each command requires you to provide the --bind parameter (with one or more endpoints).

In this chapter we will quickly demonstrate a handful of commands, to get you started. Detailed explanations for most of the commands will be given throughout the remaining chapters of this guide. Let's now see how to display the current status of the database cluster. Since we currently only have a single node configured, the command will show the status of that single node.

#### [Direct](#tab/net-cli)
```sh
./vlx status --bind localhost:7569
```

#### [Interactive](#tab/visual-studio)
```sh
bind --node localhost:7569
status
```
---
&nbsp;
&nbsp;

You should see the following output (provided you didn't change the name of the node in the config file from the default "Node"):

```accesslog
Node (Running)
```

There aren't much information displayed (other than the information that the node is running) because we did not set up a cluster. For a running cluster, much more information is displayed. VeloxDB clusters are covered in chapter [Database Cluster](database_cluster.md).

In direct mode, most commands require you to provide binding endpoint (as discussed previously) using the --bind parameter. This is not needed when binding to a database on a local machine (on default port) but was provided in the previous example for demonstration purposes. In interactive mode, you only need to execute bind command once (usually as a first command). Once a binding has been established, all commands will continue to use that binding.

In the sample application that we've built at the beginning of this chapter, you only needed to define the data model and create a single API to manipulate the model. No configuration and/or administration of the database server was necessary. This was possible due to the fact that VeloxDB package (that you added to your project) comes bundled with an instance of a VeloxDB server. When you "run" the server side logic, what happens under the hood is the server gets started for you and configured with some default parameters. This server is limited to a single node (no database cluster is possible). Other than starting the server, the run command also automatically deploys your data model and database APIs to the database. This makes it as simple as possible to quickly write and test your code (including Samples throughout this guide). In production, however, you need to perform several steps before you can start issuing requests to you APIs. First you need to configure the database persistence. This is discussed in details in chapter [Persistence](persistence.md). Here we are going to create just a single log file. This is done using the vlx tool:

#### [Direct](#tab/net-cli)
```sh
./vlx create-log --bind localhost:7569 --name MyLog --dir log_path --snapshot-dir log_path --size 5
```

#### [Interactive](#tab/visual-studio)
After binding, we first need to enter the persistence configuration sub-mode. In this sub-mode, all modifications are buffered locally and need to be applied to the server once completed.
```sh
bind --node localhost:7569
persist-config
create-log --name MyLog --dir log_path --snapshot-dir log_path --size 5
apply
exit
```
---
&nbsp;
&nbsp;

log_path is the absolute path to a directory where database will store persistence files. This directory must exist on every node of the database cluster and needs to be accessible to the database process. Once persistence has been configured, we need to deploy the data model and APIs to the database. Copy data model and API assemblies (in our sample application there is just a single server assembly) to some directory and then execute command:

#### [Direct](#tab/net-cli)
```sh
./vlx update-assemblies --bind localhost:7569 --dir assemblies_directory_path
```

#### [Interactive](#tab/visual-studio)
```sh
update-assemblies --dir assemblies_directory_path
```
---
&nbsp;
&nbsp;

Keep in mind that this command expects all assemblies that need to be deployed to the database (all model and API assemblies as well as their dependencies) to be present in the provided directory. This is especially important when updating existing state in the database, since if some assemblies (that exist in the database) are not found in the provided directory, these will be considered as deleted. This might further lead to data loss (if those assemblies contain data model classes). For this reason, update-assemblies command first displays a list of actions that will be executed (inserted/updated/deleted assemblies) and asks for the user to confirm the action. If you do not want to be asked for confirmation (for scripting purposes) you can specify --no-confirm parameter. In the case of our sample application where we had only a single assembly, you might see the output similar to the following:

```accesslog
vlxblog.dll      Inserted
Do you want to proceed (Y/N)?Y
```

## Tracing and Debugging
Given that VeloxDB executes application logic inside the database server process, there needs to be a way for developers to debug their APIs. For this, VeloxDB offers couple of options. First, as demonstrated with the sample application, you can use development deployment of the VeloxDB server that comes bundled with the VeloxDB package. When running your APIs this way, debug information (PDB files) also get loaded allowing you to easily debug your code with the debugger. When deploying the APIs to a standalone database server, PDB files are not deployed to the database.

Besides debugging your APIs with the debugger, VeloxDB provides a tracing library which allows you to trace the execution of your APIs. VeloxDB internally uses the same mechanism to trace its own execution. You can access the tracing library through a static class <xref:VeloxDB.Common.APITrace>. This class has many overloaded methods that allow you to write formatted trace messages of different trace levels. Available trace levels include Error, Warning, Info, Debug and Verbose. Following example demonstrates an API operation from the sample application modified to use tracing:

```cs
[DbAPIOperation]
public bool Update(ObjectModel om, DTO.Blog update)
{
    APITrace.Verbose("Blog with an id {0} is being updated.", update.Id);
    Blog? blog = om.GetObject<Blog>(update.Id);
    if (blog == null)
    {
        APITrace.Warning("Blog with an id {0} was not found.", update.Id);
        return false;
    }

    blog.Url = update.Url;
    return true;
}
```

With default configuration, user trace messages are not collected. You need to configure the initial trace level for user traces. This is done in the vlxdbcfg.json file by setting the UserLevel property in the Logging section. An example of a logging section inside the config file that sets the user trace level to Debug might look like this:

```cs
...
"Logging": {
    "Path": "${LocalApplicationData}/vlxdb/log",
    "Level": "Info",
    "UserLevel": "Debug"
},
...
```    

 It is also possible to modify the trace level during runtime, using the vlx client tool, by executing the following command(s):

#### [Direct](#tab/net-cli)
```sh
./vlx user-trace-level --bind localhost:7569 --node Node --level Verbose
```

#### [Interactive](#tab/visual-studio)
```sh
bind --node localhost:7569
user-trace-level --node Node --level Verbose
```
---
&nbsp;
&nbsp;

Sometimes it might be useful to increase the trace level of the internal VeloxDB trace messages (e.g. you suspect that there is a bug in VeloxDB). This is accomplished in a similar way to the previous example, by executing the following command:

#### [Direct](#tab/net-cli)
```sh
./vlx trace-level --bind localhost:7569 --node Node --level Verbose
```

#### [Interactive](#tab/visual-studio)
```sh
bind --node localhost:7569
trace-level --node Node --level Verbose
```
---
&nbsp;
&nbsp;

[1]: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token
[2]: xref:VeloxDB.ObjectInterface.ObjectModel#VeloxDB_ObjectInterface_ObjectModel_GetObject__1_System_Int64_
[3]: xref:VeloxDB.ObjectInterface.ObjectModel#VeloxDB_ObjectInterface_ObjectModel_CreateObject__1
[4]: xref:VeloxDB.ObjectInterface.DatabaseObject#VeloxDB_ObjectInterface_DatabaseObject_Id
[5]: xref:VeloxDB.ObjectInterface.DatabaseObject#VeloxDB_ObjectInterface_DatabaseObject_Delete
[6]: https://learn.microsoft.com/en-us/dotnet/csharp/roslyn-sdk/source-generators-overview
