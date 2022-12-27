---
uid: data_model
---
# Data Model

We've already went through several examples of data models and learned some of the rules how data models are defined in VeloxDB. We are now going to go through all the different capabilities that VeloxDB offers when it comes to defining your data model. Also, given that VeloxDB is an in-memory database, it is extremely important to be able to estimate system memory requirements for a given data model and dataset size.

## Classes

Classes are the main building block of your data model. You define a database class by creating an abstract .NET class that inherits from the DatabaseObject class and decorating it with the DatabaseClassAttribute. Given that VeloxDB requires for all classes to be defined as abstract .NET classes, you might be wondering how would you actually define an abstract model class? This is achieved by setting the isAbstract argument of the DatabaseClassAttribute constructor to true (default is false). You must also provide an empty constructor in your classes.

>[!NOTE]
>The reason why VeloxDB requires all model classes to be defined as abstract .NET classes is to reduce possibility of confusion which might arise if a user instantiates the class directly (without using methods provided by the ObjectModel class). This .NET object would not be associated with the ObjectModel instance (and thus with the database itself). VeloxDB generates the actual instantiable .NET classes dynamically, by inheriting from provided user abstract classes. The only way to create instances of these classes is by calling CreateObject<T> method of the provided ObjectModel instance which creates an object inside the database as well.

Following example defines a single non-abstract model class.

```cs
[DatabaseClass(isAbstract = false)]
public abstract class Order : DatabaseObject
{
}
```

### DatabaseObject and Inheritance

As already stated, each database class must inherit (somewhere along its inheritance chain) from the DatabaseObject class. This class provides some useful methods and properties:

* **Id** - Retrieves the built in 64-bit integer id, assigned to the object by the database.
* **IsDeleted** - Tells us whether we have deleted this object from the database. Deleted objects may no longer be used.
* **IsCreated** - Tells us whether we have created this object by calling the CreateObject<T> method of the ObjectModel class. Created objects can, at the same time, be deleted (IsDeleted is true) if we first create, than delete the object inside the same database operation.
* **IsAbandoned** - Tells us whether we have abandoned this object. Abandoned objects may no longer be used, and are not stored inside the ObjectModel instance, making them eligible for .NET garbage collection.
* **CanBeAbandoned** - Tells us whether it is possible to abandon an object. Objects that have been either created or modified inside the current database operation may not be abandoned.
* **IsSelected** - Tells us whether an object has been selected for automatic mapping into DTOs. More details about this can be found in chapter TODO.
* **Owner** - Retrieves the instance of ObjectModel class that owns this object. This is the same instance that was provided at the beginning of the database operation.
* **Select()** - Marks an object as being selected for automatic mapping into DTOs.
* **Delete()** - Deletes an object from the database.
* **Abandon()** - Abandons a .NET object making it eligible for garbage collection.

You are free to create arbitrary class hierarchies inside VeloxDB (as long as the aforementioned rule of inheriting the DatabaseObject at some point is satisfied). The following example demonstrates this:

```cs
[DatabaseClass(isAbstract = true)]
public abstract class Company : DatabaseObject
{
}

[DatabaseClass]
public abstract class Supplier : Company
{
}

[DatabaseClass]
public abstract class Customer : Company
{
}
```

### Class Properties



#### Simple Properties

#### Strings and Arrays

#### References

#### Estimating Class Memory Requirements

### Inverse References

#### Estimating Inverse Reference Memory Requirements

## Hash Indexes

#### Estimating Hash Index Memory Requirements