---
uid: start
---
## Simple Application
The application consists of two parts, building a server side library and building a client application.

### Set up NuGet source
While VeloxDB is in private beta, packages will be hosted on GitHub Packages. In order to access packages, you will need to add an additional package source to your NuGet setup. Since these packages are private you will need to authenticate yourself to GitHub using personal access token. For information how to obtain personal access token see [Creating a personal access token][1].

#### [.NET CLI](#tab/net-cli)
```sh
dotnet nuget add source -n "velox" https://nuget.pkg.github.com/defufna/index.json --username USERNAME --password TOKEN
```
On Linux/Mac platforms you will also need to use ```--store-password-in-clear-text``` since dotnet does not support encryption on non-windows platforms.

#### [Visual Studio](#tab/visual-studio)
* Open Visual Studio
* Go to Tools 🡒 Options 🡒 Nuget Package Manager 🡒 Package Sources
* Click the + sign
* Enter **Velox** for the name and **https://nuget.pkg.github.com/defufna/index.json** for source
* Click update
* Click ok
* Visual Studio will prompt you for your username and password (use personal access token) when you first try to access package source
---
&nbsp;
&nbsp;

>[!NOTE]
>Expect these packages to move to public nuget.org library after the release.

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
* Select **.NET 6.0** for framework and click **Create**.
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
dotnet add package veloxdb
```

#### [Visual Studio](#tab/visual-studio)

* Open Project 🡒 Manage NuGet Packages
* Select **Velox** in **Package source** dropdown list
* Click **Browse** and in Search type in **VeloxDB**
* Click on the VeloxDB package and click on **Install**
---

#### Define the model

Model is defined using regular C# classes. Add a Model.cs file to your project with the following content:

[!code-csharp[Main](../../Samples/GetStarted/VlxBlog/Model.old.cs)]

Let's take a closer look at the classes defined here. Model classes must be declared as abstract, this is also true for all database properties. This allows VeloxDB to dynamically create implementation of classes at runtime.

Model classes must inherit from the <xref:Velox.ObjectInterface.DatabaseObject> class, which defines common properties and methods for all database classes (for example [Id][4] and [Delete][5]).

Classes and properties must be marked with attributes <xref:Velox.ObjectInterface.DatabaseClassAttribute> and <xref:Velox.ObjectInterface.DatabasePropertyAttribute> respectively. Also note <xref:Velox.ObjectInterface.InverseReferencesAttribute> attribute. You use this attribute to define inverse reference properties. In this case post has a reference to blog, using <xref:Velox.ObjectInterface.InverseReferencesAttribute> we declare a property that allows us to easily get all posts of a single blog.

Post.Blog property is marked with <xref:Velox.ObjectInterface.DatabaseReferenceAttribute>, which we use to specify additional information about the reference. The first argument specifies whether the reference can be null. In this case it doesn't make sense to have a post without a blog. The second argument specifies what should a database do when the target object is deleted. In case a blog is deleted, we want all of its posts to be deleted as well, so we set it to cascade delete. The last argument specifies whether the database will track inverse references for this reference. Since we plan to navigate this reference in reverse direction (from blog to post) we set this to true.

#### Create DTOs
We will now define DTO classes, which will be used to transfer data between the client and the server. Create DTO.cs file with the following content:

[!code-csharp[Main](../../Samples/GetStarted/VlxBlog/DTO.cs)]

For more information about why we use DTOs see [Data Transfer Objects](tech.md#data-transfer-objects).

#### Create ToDTO and FromDTO methods
Copying data between model and DTOs can be cumbersome. For that reason VeloxDB comes with builtin automapper that can save you from typing some boilerplate code. To use automapper make Blog and Post database classes in Model.cs partial. Also add highlighted methods to Blog and Post.

[!code-csharp[Main](../../Samples/GetStarted/VlxBlog/Model.cs?highlight=16-18,33-35)]

Automapper is implemented using [C# Source Generators][6]. The partial methods you created will be implemented by the source generator at compile time. There are two types of methods: To and From methods. To create an automapper method you need to add a partial method to the database class. Currently automapper methods are only supported for database classes. The method needs to start with an appropriate prefix (To/From). It also has to have correct arguments and return type. You can see the specific requirements for each method type in the table below.

| Method Prefix | Description           | Is static | Arguments        | Return type |
|---------------|-----------------------|-----------|------------------|-------------|
| To            | Copies DBO to DTO     | No        | No arguments     | DTO         |
| From          | Creates DBO from DTO  | Yes       | ObjectModel, DTO | DBO         |

For more detailed information see [Automapper](guide/automapper.md) section of the guide.

#### Create database API
All business logic is defined in server side methods, we call these methods database operations. By keeping the business logic and the data in the same process, VeloxDB avoids multiple round trips to the Database. Create BlogApi.cs file with the following content:
[!code-csharp[Main](../../Samples/GetStarted/VlxBlog/BlogApi.cs)]

The methods described in this file can be called from the client using Velox protocol library. This will be covered in the next chapter. To define a database API operation you need to create a class marked with the <xref:Velox.Protocol.DbAPIAttribute>. Each method should be decorated with the <xref:Velox.Protocol.DbAPIOperationAttribute>.

For more information see [VeloxDB Protocol](tech.md#veloxdb-protocol).

The first argument of all Database operations must be of type <xref:Velox.ObjectInterface.ObjectModel>. <xref:Velox.ObjectInterface.ObjectModel> is used to access the data in the database. In this example we use the [GetObject][2] method to read objects, by Id, from the database and [CreateObject][3] to create new objects.

Each operation corresponds to a single transaction inside the database. While an operation is running, changes it makes to the model are not visible to other operations. After an operation completes successfully its changes are committed to the database automatically. If an operation throws an exception all changes are rolled back. If your operation only reads data from the database, you can set <xref:Velox.Protocol.DbAPIOperationAttribute.OperationType> to <xref:Velox.Protocol.DbAPIOperationType.Read>. For more information about transactions see [Transactions](tech.md#transactions).

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
22-06-24 11:05:13.314 [1B22] [Info] Database 0 restored.
22-06-24 11:05:13.487 [1B22] [Info] Database 1 restored.
22-06-24 11:05:13.548 [1B22] [Info] Storage engine started.
22-06-24 11:05:13.580 [1B22] [Info] Looking for changes in C:\VeloxDB\vlxblog\bin\x64\Debug\net6.0\
22-06-24 11:05:13.590 [1B22] [Info] New assembly: vlxblog.dll
22-06-24 11:05:13.797 [1B22] [Info] User assemblies updated.
22-06-24 11:05:13.805 [1B22] [Info] Initializing persistence to C:\VeloxDB\vlxblog\bin\x64\Debug\net6.0\vlxdata.
22-06-24 11:05:13.939 [1B22] [Info] Persistence configuration updated.
22-06-24 11:05:13.941 [1B22] [Info] Server successfully started.
```

VeloxDB NuGet package comes bundled with the VeloxDB server. When you run your project, the server starts, loads the assembly and creates the database if it doesn't already exist. The database files are placed in project's output directory in vlxdata subdirectory. This is the development deployment. For the recommended production deployment please see [Deploying VeloxDB](tech.md#deploying-veloxdb).

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
dotnet add package velox
```

#### [Visual Studio](#tab/visual-studio)

* Open Project 🡒 Manage NuGet Packages
* Select **Velox** in **Package source** dropdown list
* Click **Browse** and in Search type in **Velox**
* Click on the Velox package and click on **Install**
---

Velox package is used for calling public database APIs.

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

The interface must be marked with the <xref:Velox.Protocol.DbAPIAttribute>. Note the name parameter, as it specifies the name of the database API we target with this interface. Default name for a database API is the full .NET name of the API class. Then we specify all operations we have defined on the server. Note that operations omit ObjectModel argument here. ObjectModel is injected server side and is not needed (or available) client side. Each operation is marked with the <xref:Velox.Protocol.DbAPIOperationAttribute>.

#### Create main method

Replace the content of Program.cs with the following:

[!code-csharp[Main](../../Samples/GetStarted/VlxClient/Program.cs)]

We first create an instance of <xref:Velox.Client.ConnectionStringParams>. This is a helper class that will help us create the connection string. We add our server's address to it (VeloxDB's default port is 7568). Then we use <xref:Velox.Client.ConnectionFactory> to get the connection to the server. Once we have the connection, calling database operations is trivial, we just call appropriate methods.

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

[1]: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token
[2]: xref:Velox.ObjectInterface.ObjectModel#Velox_ObjectInterface_ObjectModel_GetObject__1_System_Int64_
[3]: xref:Velox.ObjectInterface.ObjectModel#Velox_ObjectInterface_ObjectModel_CreateObject__1
[4]: xref:Velox.ObjectInterface.DatabaseObject#Velox_ObjectInterface_DatabaseObject_Id
[5]: xref:Velox.ObjectInterface.DatabaseObject#Velox_ObjectInterface_DatabaseObject_Delete
[6]: https://learn.microsoft.com/en-us/dotnet/csharp/roslyn-sdk/source-generators-overview