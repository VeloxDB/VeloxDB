---
uid: data_model
---
# Data Model

We've already went through several examples of data models and learned some of the rules how data models are defined in VeloxDB. We are now going to go through all the different capabilities that VeloxDB offers when it comes to defining your data model. Also, given that VeloxDB is an in-memory database, it is extremely important to be able to estimate system memory requirements for a given data model and dataset size, so we'll cover those as well.

## Classes

Classes are the main building blocks of your data model. You define a database class by creating a public abstract .NET class that inherits from the <xref:VeloxDB.ObjectInterface.DatabaseObject> class and is decorated with the <xref:VeloxDB.ObjectInterface.DatabaseClassAttribute> attribute. Given that VeloxDB requires for all classes to be defined as abstract .NET classes, you might be wondering how to model an actual abstract class in the database? This is achieved by setting the isAbstract argument of the DatabaseClassAttribute constructor to true (default is false). One additional requirement is that the provided .NET class defines an empty constructor.

>[!NOTE]
>The reason why VeloxDB requires all .NET classes to be defined as abstract is to reduce possibility of confusion which might arise if a user instantiates the class directly (without using methods provided by the ObjectModel class). This .NET object would not be associated with the ObjectModel instance (and thus with the database itself). VeloxDB generates the actual instantiable .NET classes dynamically, by inheriting from provided user abstract classes. The only way to create instances of these classes is by calling <xref:VeloxDB.ObjectInterface.ObjectModel.CreateObject*> method of the provided ObjectModel instance which creates an object inside the database as well.

Following example defines a single empty (non-abstract) model class.

```cs
[DatabaseClass(isAbstract = false)]
public abstract class SalesOrder : DatabaseObject
{
}
```

### DatabaseObject and Inheritance

As already stated, each database class must inherit (somewhere along its inheritance chain) from the DatabaseObject class. This class provides some useful methods and properties:

* **<xref:VeloxDB.ObjectInterface.DatabaseObject.Id>** - Retrieves the built in 64-bit integer id, assigned to the object by the database. This id is unique among all objects of all classes in the database.
* **<xref:VeloxDB.ObjectInterface.DatabaseObject.IsDeleted>** - Indicates whether we have deleted this object from the database. Deleted objects may no longer be used.
* **<xref:VeloxDB.ObjectInterface.DatabaseObject.IsCreated>** - Indicates whether the object was created during the execution of the current database operation (by a call to the CreateObject\<T\> method of the ObjectModel class). Created objects can, at the same time, be deleted (IsDeleted is true) if we first create, than delete the object inside the same database operation.
* **<xref:VeloxDB.ObjectInterface.DatabaseObject.IsAbandoned>** - Indicates whether we have abandoned this object. Abandoned objects may no longer be used, and are not stored inside the ObjectModel instance, making them eligible for .NET garbage collection.
* **<xref:VeloxDB.ObjectInterface.DatabaseObject.CanBeAbandoned>** - Indicates whether it is possible to abandon the object. Objects that have been either created or modified inside the current database operation may not be abandoned.
* **<xref:VeloxDB.ObjectInterface.DatabaseObject.IsSelected>** - Indicates whether the object has been selected for automatic mapping into DTOs. More details about this can be found in chapter [Database APIs](database_apis.md#automapper).
* **<xref:VeloxDB.ObjectInterface.DatabaseObject.Owner>** - Retrieves the ObjectModel instance that owns this object. This is the same instance that was provided at the beginning of the database operation.
* **<xref:VeloxDB.ObjectInterface.DatabaseObject.Select>** - Marks the object as being selected for automatic mapping into DTOs.
* **<xref:VeloxDB.ObjectInterface.DatabaseObject.Delete>** - Deletes the object from the database.
* **<xref:VeloxDB.ObjectInterface.DatabaseObject.Abandon>** - Abandons the .NET object making it eligible for garbage collection.

You are free to create arbitrary class hierarchies inside VeloxDB (as long as the aforementioned rule of inheriting the DatabaseObject at some point is satisfied). The following example demonstrates usage of inheritance:

```cs
[DatabaseClass(isAbstract = true)]
public abstract class Entity : DatabaseObject
{
}

[DatabaseClass]
public abstract class LegalEntity : Entity
{
}

[DatabaseClass]
public abstract class Person : Entity
{
}
```

When inheriting classes, similar rules apply as with the inheritance in the .NET itself. A class can only inherit a single other class (no multiple inheritance). Abstract class may not inherit from a non abstract class. Generic classes are not supported by VeloxDB.

### Class Properties

Each class can contain zero or more database properties. Database properties (equivalent to columns in relational models) are defined as abstract .NET properties decorated with an appropriate attribute (more on this later). These properties must contain both getters and setters (both public). Again, properties are defined as abstract because VeloxDB object interface provides implementations for these properties that read the values directly from the database. Thus, these properties do not actually consume any memory, at least not until they are modified. VeloxDB supports following types of properties: simple properties, strings, arrays of simple types and references.

#### Simple Properties

Simple properties represent values of simple types. Following is a list of supported types:
* **Byte (byte)** - 8-bit unsigned integer value.
* **Int16 (short)** - 16-bit signed integer value.
* **Int32 (int)** - 32-bit signed integer value.
* **Int64 (long)** - 64-bit signed integer value.
* **Single (float)** - 32-bit floating point number.
* **Double (double)** - 64-bit floating point number.
* **Boolean (bool)** - true/false boolean value (1 byte in size).
* **DateTime** - A point in time represented by the number of 100-nanosecond ticks since 12:00 midnight, January 1, 0001 A.D. (C.E.) in the Gregorian calendar. There is no separate type in VeloxDB for storing just the date (without time).
* **Enumeration** - A set of possible constant values (with a corresponding integer representation in the database).

>[!NOTE]
>Support for 128-bit decimal types is coming in the near future.

As with many object oriented languages, simple values cannot be null (unlike values in relational databases). If you need to model a simple value that can be undefined/unknown, you can either define a separate property to hold that information or use specific values from the available range to represent those special values.

If you define a simple property of enumeration type, given enumeration type must have an underlying .NET type from the following set: Byte (byte), Int16 (short), Int32 (int) or Int64 (long).

You define a simple property by defining an abstract .NET property and decorating it with a <xref:VeloxDB.ObjectInterface.DatabasePropertyAttribute> attribute. This attribute allows you to assign a default value to the property by specifying its property <xref:VeloxDB.ObjectInterface.DatabasePropertyAttribute.DefaultValue>. Do not attempt to assign a default value to a database property in any other way, for example by assigning a value in the constructor of the class. This will result in an InvalidOperationException being thrown. Default value for a DateTime property must be specified in one of the following formats: yyyy-MM-dd HH:mm:ss.fff, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm, yyyy-MM-dd.

>[!NOTE]
>The reason why you are not allowed to set default property values in the constructor of your class is because ObjectModel calls this constructor when you read objects from the database. You wouldn't want all the properties to be reset to their default values whenever you read an object from the database.

Let's now enhance the previously defined data model with some properties:

```cs
public enum LegalEntityType : byte
{
    LLC = 1,
    Corporation = 2,
    ...
}

[DatabaseClass(isAbstract = true)]
public abstract class Entity : DatabaseObject
{
    [DatabaseProperty]
    public abstract double CreditAmount { get; set; }

    [DatabaseProperty]
    public abstract DateTime LastLoginTime { get; set; }
}

[DatabaseClass]
public abstract class LegalEntity : Entity
{
    [DatabaseProperty]
    public abstract long TaxNumber { get; set; }

    [DatabaseProperty(defaultValue:"LLC")]
    public abstract LegalEntityType Type { get; set; }
}

[DatabaseClass]
public abstract class Person : Entity
{
    [DatabaseProperty]
    public abstract DateTime DateOfBirth { get; set; }
}
```

#### Strings

Properties of type String (string) are defined in a similar way as simple properties. There are some difference though. One difference is that VeloxDB does not allow specifying the default value for a string property. This is actually the case for all types of properties except simple properties. One additional difference is that string properties are always nullable (with null being the default value for these properties). Additionally, there is no way to specify a maximum allowed length for a string value. Maximum theoretical limit is imposed by the .NET itself by limiting the size of any object to 2GB. Since strings use UTF-16 encoding (2 bytes per character) this leaves the maximum string length at 1,073,741,823 characters.

We will now define some string properties inside the existing data model, and also extend the model with some additional classes:

```cs
public enum LegalEntityType : byte
{
    LLC = 1,
    Corporation = 2,
    ...
}

public enum OrderStatus : byte
{
    Pending = 1,
    BeingProcessed = 2,
    Shipped = 3,
    Completed = 4,
}

[DatabaseClass(isAbstract = true)]
public abstract class Entity : DatabaseObject
{
    [DatabaseProperty]
    public abstract string UserName { get; set; }

    [DatabaseProperty]
    public abstract string PasswordHash { get; set; }

    [DatabaseProperty]
    public abstract string Email { get; set; }

    [DatabaseProperty]
    public abstract double CreditAmount { get; set; }

    [DatabaseProperty]
    public abstract DateTime LastLoginTime { get; set; }

    [DatabaseProperty]
    public abstract string ShippingAddress { get; set; }
}

[DatabaseClass]
public abstract class LegalEntity : Entity
{
    [DatabaseProperty]
    public abstract string Name { get; set; }

    [DatabaseProperty]
    public abstract long TaxNumber { get; set; }

    [DatabaseProperty(defaultValue:"LLC")]
    public abstract LegalEntityType Type { get; set; }
}

[DatabaseClass]
public abstract class Person : Entity
{
    [DatabaseProperty]
    public abstract DateTime DateOfBirth { get; set; }

    [DatabaseProperty]
    public abstract string FirstName { get; set; }

    [DatabaseProperty]
    public abstract string LastName { get; set; }
}

[DatabaseClass]
public abstract class SalesOrder : DatabaseObject
{
    [DatabaseProperty]
    public abstract OrderStatus Status { get; set; }

    [DatabaseProperty]
    public abstract DateTime CompletionTime { get; set; }
}

[DatabaseClass]
public abstract class Product : DatabaseObject
{
    [DatabaseProperty]
    public abstract string Name { get; set; }

    [DatabaseProperty]
    public abstract string Description { get; set; }
}
```

#### Arrays

VeloxDB allows you to define arrays of simple values and arrays of strings. These are called array properties. Similar to string properties, array properties are nullable and do not allow default value (which is always null). Again, similar to string properties, it is not possible to define maximum array length (the only limit is coming from the the .NET itself). Array properties are defines in a similar way as any other properties in VeloxDB, as public .NET properties with getter and setter, decorated with DatabasePropertyAttribute attribute. Property type of these properties, however, must be <xref:VeloxDB.ObjectInterface.DatabaseArray`1> where T is any of the simple types supported by VeloxDB plus the string type. Here se an example of an array property of double values.

```cs
[DatabaseProperty]
public abstract DatabaseArray<double> Value { get; set; }
```

DatabaseArray\<T\> implements IList\<T> interface so it behaves similarly to a list. All methods have the same time complexity as equivalent methods on a List\<T\> class. These are same of the most common properties and methods of the DatabaseArray<T> class:
* **<xref:VeloxDB.ObjectInterface.DatabaseArray`1.Count>** - Number of elements in the array. Time complexity is O(1).
* **[this\[\]](xref:VeloxDB.ObjectInterface.DatabaseArray`1.Item(System.Int32))** - Provides indexed access to an element of the array. Time complexity is O(1).
* **<xref:VeloxDB.ObjectInterface.DatabaseArray`1.Add*>** - Adds an element to the end of the array (expands the size of the array if needed). Time complexity is O(1).
* **<xref:VeloxDB.ObjectInterface.DatabaseArray`1.Clear>** - Clears the array (removes all elements). Time complexity is O(n).
* **<xref:VeloxDB.ObjectInterface.DatabaseArray`1.Insert*>** - Inserts an element at a given position. Time complexity is O(n).
* **<xref:VeloxDB.ObjectInterface.DatabaseArray`1.RemoveAt*>** - Removes an element at a given position. Time complexity is O(n).

>[!CAUTION]
>It is very important to understand how VeloxDB handles array properties. When you access an array property, a copy of the array from the database is made and stored inside the ObjectModel instance. All your actions against the array are executed against that copy. Once the transaction is committed, array inside the database is replaced by the local copy from the ObjectModel. For this reason, working with long arrays may produce significant cost given that every database operation needs its own copy of the array. This can be especially expensive if the most common operations is only a slight array modification. For example, if, every time a database operation is called, you add another element at the end of the array. This will take O(n^2) time to fill an array. Consequently, try to keep your arrays relatively short. At some point it might be better to just model array elements as a separate class. If, on the other hand, you mostly replace an entire array in each operation than using an array property is optimal.

Besides modifying an existing array (retrieved from the database) you are free to assign new arrays to a an array property. You create new arrays by calling the static <xref:VeloxDB.ObjectInterface.DatabaseArray`1.Create*> method of the DatabaseArray\<T\> class. Following example demonstrates this (assuming we have an object with the double array property from the previous example):

```cs
var v = DatabaseArray<double>.Create();
v.Add(1.0);
v.Add(2.0);
v.Add(3.0);
obj.Value = v;
```

#### References

Reference properties allow you to form relationships between classes. Reference in VeloxDB can have any of the following cardinalities:
* Zero to One (0..1) - Single instance of the referencing class can be associated with no more than one instance of the referenced class. Reference property with this cardinality is nullable (you can assign a null reference to it).
* One (1) - Single instance of the referencing class is associated with exactly one instance of the referenced class. Reference property with this cardinality must have a non-null value set once a database operation is complete. It can, however, contain a null value during the operation execution.
* Many (\*) - Single instance of the referencing class is associated with an arbitrary number of instances of the referenced class. This reference property actually represents an array of references.

You define a reference property of cardinality 0..1 and 1 by defining a .NET property whose type is the referenced class, and decorating it with the <xref:VeloxDB.ObjectInterface.DatabaseReferenceAttribute> attribute. Distinguishing between 0..1 and 1 cardinalities is done by specifying the true/false value for the <xref:VeloxDB.ObjectInterface.DatabaseReferenceAttribute.IsNullable> of the DatabaseReferenceAttribute attribute. Let us enhance the previously defined sales order model with some references (we will not repeat entire classes here to keep the example compact):

```cs
[DatabaseClass]
public abstract class SalesOrder : DatabaseObject
{
    [DatabaseReference(isNullable: false, deleteTargetAction: DeleteTargetAction.PreventDelete)]
    public abstract Entity OrderedBy { get; set; }

    ...
}
```

VeloxDB strictly maintains referential integrity. As already mentioned, references of cardinality 1 are not allowed to be committed to the database if their value is null. Also, each reference that points to a non-null object must point to an existing database object (of appropriate type). When you attempt to delete an object that is being referenced by some other object, the database needs to determine what to do with that reference (since a reference cannot point to a deleted object). This is where the <xref:VeloxDB.ObjectInterface.DatabaseReferenceAttribute.DeleteTargetAction> property, of the DatabaseReferenceAttribute attribute comes into play. It can take any of the following values:
* PreventDelete - The database will prevent any attempt to delete the referenced object.
* CascadeDelete - The referencing object is deleted as well. This can continue to propagate throughout the database (hence the name cascade delete).
* SetToNull - The referencing property is set to null. This value is invalid for a property of cardinality 1, since it cannot have a null value. If the reference property is of cardinality *, than that reference is simply removed from the reference array.

References with cardinality * are defined in a similar way to the references with cardinality 0..1/1. The only difference is the property type which is required to be <xref:VeloxDB.ObjectInterface.ReferenceArray`1> where T is the referenced class. This class implements IList\<T\> interface (similarly to DatabaseArray\<T\>). You create instances of this class by using any of the publicly available constructors. Following example extends the SalesOrder class to contain a list of ordered products:

```cs
[DatabaseClass]
public abstract class SalesOrder : DatabaseObject
{
    [DatabaseReference(isNullable: false, deleteTargetAction: DeleteTargetAction.PreventDelete)]
    public abstract Entity OrderedBy { get; set; }

    [DatabaseReference(deleteTargetAction: DeleteTargetAction.PreventDelete)]
    public abstract ReferenceArray<Product> Products { get; set; }

    ...
}
```

>[!CAUTION]
>VeloxDB handles reference arrays in a similar way how it handles arrays of simple values. For the same reason, having very large arrays of references is not recommended (unless you predominantly modify entire arrays in each operation) since the overhead of creating an array copy, just so you can modify a few values, might be too high.

As previously demonstrated, VeloxDB references support polymorphism, meaning you can reference a base class (optionally abstract class) and assign an object of a derived class to that reference. When you read a polymorphic reference, if needed, you can examine the actual type of the retrieved object. However, be careful to not use the exact type comparison like in the following example:

```cs
Entity e = order.OrderedBy;
if (e.GetType() == typeof(Person))
{
    ...
}
```

Given comparison will never return true because the actual type of the returned object is not Person (since it is an abstract class) but an internally generated class that inherits from Person. The following example demonstrates the proper way to examine the returned type:

```cs
Entity e = order.OrderedBy;
if (e is Person)
{
    ...
}
```

#### Non Database Properties

Besides defining database properties in your .NET classes, you are free to declare other properties that are ignored by the database. For example, lets say we are trying to implement a graph search algorithm and want to be able to mark graph nodes that have been visited (but do not want to keep this information in the database, obviously). We can  define a non-database property:

```cs
[DatabaseClass]
public abstract class GraphNode : DatabaseObject
{
    public bool Visited { get; set; }
    ...
}
```

You can initialize and modify this property freely, and can even abandon an object after modifying it. Note, however, that this property will increase the size of the .NET object in memory (size in the database will be unaffected) and this will affect all the database operations that use objects of this class even though these operations might not need that specific property.

### Estimating Class Memory Requirements

Since VeloxDB stores all data in system memory, it is very important to be able to estimate how much memory a particular class will use. Usually this estimation is important only for the classes that will have large number of instances. Keep in mind that we are not talking about the size of the .NET objects that might get created by the object interface. Those objects should usually be much fewer in numbers than the database objects. Remember to use abandon option on .NET objects that are no longer needed, to try to keep the number of locally cached .NET objects to a minimum. This is especially useful for database operations that read many objects from the database (but modify only a portion of them).

The basic formula for estimating the size of a single object in memory (in bytes) is the following:

```
objectSize = 68 + databasePropertiesSize
```

Keep in mind that the 68 bytes in the formula includes the built-in 64-bit id as well as the built-in hash index for that id. In that sense the actual overhead per record is 44 bytes. To calculate the size of the database properties you simply add up the size of every individual property. Different property types have different sizes:
* Byte - 1 byte
* Int16 - 2 bytes
* Int32 - 4 bytes
* Int64 - 8 bytes
* Single - 4 bytes
* Double - 8 bytes
* Boolean - 1 byte
* DateTime - 8 bytes
* Reference with cardinality 0..1 or 1 - 8 bytes

The size of a string property is calculated using the following formula:

```
stringSize = 52 + length * 2
```

The size of an array property is calculated using the following formula:

```
arraySize = 12 + length * sizeof(element)       // Where sizeof(element) is the size of a single array element
```

Now to calculate the total size of a single class you need to estimate the number of instances of that class. Also, remember that VeloxDB uses Multi Versioning Concurrency Control mechanisms to ensure transaction isolation. This essentially means that for each object, there might be additional (old) versions of that object, present in memory (until Garbage Collector is able to collect them). To estimate the number of old versions in memory present at any point in time we need to know the maximum rate of change for that class as well as the maximum expected duration of a single transaction (database operation). Lets say that the rate of change of a given class is changeRate (expressed in updates per second) and that the maximum transaction duration is maxTranDuration (expressed in seconds). The formula for total number of object instances of a given class is given as:

```
objectCount = expectedObjectCount + changeRate * maxTranDuration
```

We will now do couple of examples from the previously defined sales and orders data model. Let us first calculate the estimated size of the Person class in memory given following assumptions: number of persons in the database is 500k, average length of all the strings is 16 bytes, the rate of change of Person class is 10 changes/s and the longest transaction in the database is 10 sec.

```
propertiesSize = 3 * 8 + 6 * (52 + 16 * 2) = 526 b
size = (500k + 10 * 10) * (68 + 526) ~ 297 MB
```

Now lets check the estimation for the SalesOrder class given the following assumptions: maximum number of order instances 100 million, rate of change is 100k changes/s.

```
propertiesSize = 2 * 8 = 16 b
size = (100M + 100k * 10) * (68 + 16) ~ 7.9 GB
```

>[!NOTE]
>Estimating the size of all the classes is obviously not enough to estimate the total memory usage for an entire server. Besides classes you must estimate the memory requirements of all the indexes in the database (which will be explained in the following sections of this chapter) as well as the memory usage of all the active operations that are being executed against the database. This last one might be difficult (especially if you have particularly large queries) so it might be best to test that.

## Inverse References

For each reference property you define, VeloxDB maintains an inverse reference index which allows you to quickly navigate the reference in the reverse direction. In our data model example, each SalesOrder instance has a reference to an Entity that placed the order as well as an array of references of all the products that are part of that order. If you wanted to quickly obtain all the orders that a single entity placed, as well as all the orders that included a certain product, you could define the inverse reference properties:

```cs
[DatabaseClass]
public abstract class Entity : DatabaseObject
{
    [InverseReferences(nameof(SalesOrder.OrderedBy))]
    public abstract InverseReferenceSet<SalesOrder> Orders { get; }
    ...
}

[DatabaseClass]
public abstract class Product : DatabaseObject
{
    [InverseReferences(nameof(SalesOrder.Products))]
    public abstract InverseReferenceSet<SalesOrder> Orders { get; }
    ...
}
```

Inverse reference property is defined by defining a .NET property (without a setter) of type <xref:VeloxDB.ObjectInterface.InverseReferenceSet`1> where T is the referencing class. This property needs to be defined in the class that is referenced by the reference property. In our case, SalesOrder class references Entity with the OrderedBy reference, so the inverse reference property must be defined in the Entity class and must be of type InverseReferenceSet\<SalesOrder\>. Also, inverse reference property must be decorated with the <xref:VeloxDB.ObjectInterface.InverseReferencesAttribute> attribute where you specify the name of the reference property for which to create the inverse reference property. Since this represents the name of the property from the referencing class, we strongly recommend using the nameof expression to avoid repeating the property name.

InverseReferenceSet\<T\> class represents an unordered collection (a set) of objects. You should never rely on the order of entities in this set remaining the same between different operations (transactions). The database is free to reorder this list any way it sees fit. This is similar to reading from a relational database without specifying an order by clause. Even though the returned order might always be the same, it is not guaranteed.

Some of the most commonly used properties and methods of InverseReferenceSet\<T\> are:
* **<xref:VeloxDB.ObjectInterface.InverseReferenceSet`1.Count>** - Number of elements in the set. Time complexity is O(1).
* **[this\[\]](xref:VeloxDB.ObjectInterface.InverseReferenceSet`1.Item(System.Int32))** - Provides indexed access to an element of the set. Time complexity is O(1).
* **<xref:VeloxDB.ObjectInterface.InverseReferenceSet`1.Add*>** - Adds an element to the set. Time complexity is O(1).
* **<xref:VeloxDB.ObjectInterface.InverseReferenceSet`1.Remove*>** - Removes the first occurrence of the specified element from the set. Time complexity is O(n).
* **<xref:VeloxDB.ObjectInterface.InverseReferenceSet`1.Clear>** - Clears the set (removes all elements). Time complexity is O(n).

It is possible that the same object appears more than once in the inverse reference set. This can occur if a reference array property contains the same reference multiple times.

Even though the InverseReferenceSet\<T\> class contains methods that allow you to directly modify the set, you should always prefer to modify the referencing property instead. For example, if we wanted to remove a product from the order, you could locate the product and remove a given order from its Orders set, but it makes more sense (and is a more performant option) to locate the order and remove the product from its Products array.

VeloxDB maintains inverse references for each reference property, by default. This is true even if you do not create an inverse reference property. If you do not want to maintain this index for some property, you can set the <xref:VeloxDB.ObjectInterface.DatabaseReferenceAttribute.TrackInverseReferences> property of the DatabaseReferenceAttribute attribute to false, when defining the reference property. For example, if we wanted to exclude the reference OrderedBy of the SalesOrder class, from the inverse references index, this is how we would accomplish this:

```cs
[DatabaseClass]
public abstract class SalesOrder : DatabaseObject
{
    [DatabaseReference(isNullable: false, deleteTargetAction: DeleteTargetAction.PreventDelete, trackInverseReferences: false)]
    public abstract Entity OrderedBy { get; set; }

    ...
}
```

>[!NOTE]
>In the current implementation of the inverse reference index, even the references that are excluded from the inverse reference index produce some overhead. This is planned to be fixed in the near future.

>[!CAUTION]
>Excluding a reference property from the inverse reference index needs to be done with extreme caution. This can severely impact the performance of delete operations of the referenced class. Whenever an object is being deleted from the database, the database will check the inverse reference index to see if there are objects in the database that reference the deleted object. If that is the case, these references need to be handled, before the delete operation is allowed. If a reference property is excluded from the index, database needs to perform a class scan (equivalent to full table scan in relational databases) to check whether there is an object referencing the deleted object. In our last example, if we were to delete an Entity object, the database would have to scan through an entire SalesOrder class to verify that no order is currently referencing the deleted entity. We already talked about how large class scans can create problems when executing inside a read-write transaction, so be sure to test your specific use case.

### Estimating Inverse Reference Memory Requirements

Estimating the size of the inverse reference index is much more difficult than what was the case for classes. Total size of the index is largely dependent on the distribution of the references. If most references target only a portion of the objects in the database, the total memory requirements are significantly lower than if the references target all the objects evenly. Having said that, it is still important to be able to calculate a rough estimate of the memory requirements of the inverse reference index. Let's assume that we can estimate the following values for each reference property in the data model:
* c[i] - Total number of references for the i-th property. If the reference cardinality is 0..1 or 1, upper bound on the total number of references is equal to the total number of objects that have this reference. If the reference cardinality is * than this number is equal to the number of objects multiplied by the average number of references per object.
* d[i] - Average number of distinct objects targeted by the i-th reference property. If the distribution of the references is uniform, than this number is equal to c[i] / referencedObjectCount. Otherwise it might be difficult to estimate this number.

If we also know the rate of changes of reference values per second, refChangeRate, we can finally estimate the size of the inverse reference index with the following formula:

```
size = sum(c[i] / d[i]) * 64 + refChangeRate * maxTranDuration * 112 + sum(c[i]) * 12
```

We provide this estimation formula for the sake of completeness. It might come in handy for the initial analysis, before a realistic data set can be obtained. However, testing with real data at some point is highly recommended.

## Hash Indexes

You've already seen how VeloxDB provides specialized indexes for specific use cases (e.g. hash index on the Id property and inverse reference index). On top of that, users are able to create additional hash indexes on arbitrary set of properties, with some limitations. Array properties may not be indexed. This essentially means that you can index simple properties, string properties and reference properties of cardinality 0..1 and 1. However, creating hash indexes on reference properties makes little sense (given the existence of the inverse reference index) except maybe in situations where you need to index multiple properties. Another limitation is the maximum number of indexed properties in a single hash index, which is currently set to four.

>[!NOTE]
>Hash indexes can only be used for equality comparisons. There is a plan to introduce a sorted index in the near future which would provide speedup for range queries and sorting operations.

Hash indexes are defined on database classes and get inherited by the derived classes. You define a hash index by decorating a database class with a <xref:VeloxDB.ObjectInterface.HashIndexAttribute> attribute. First argument of the HashIndexAttribute constructor is the name of the hash index. The namespace name of the indexed class is combined with the provided index name to generate full index name. Full index name must be unique in the database. Second parameter, specifies whether a uniqueness constraint should be enforced on the index. If set to true, the database will prevent any attempts to insert a key that already exists. Uniqueness constraint is enforced for the defining class as well as any descendant classes, meaning that the key needs to be unique for all these classes. The remaining arguments of the HashIndexAttribute constructor are the names of indexed properties (up to four names). Let's now define some hash indexes on our previously defined data model:

```cs
[DatabaseClass(isAbstract = true)]
[HashIndex(UserNameIndex, true, nameof(UserName))]
public abstract class Entity : DatabaseObject
{
    public const string UserNameIndex = "UserNameIndex";

    [DatabaseProperty]
    public abstract string UserName { get; set; }

    ...
}

[DatabaseClass]
[HashIndex(TaxNumberIndex, true, nameof(TaxNumber))]
public abstract class LegalEntity : Entity
{
    public const string TaxNumberIndex = "TaxNameIndex";

    [DatabaseProperty]
    public abstract long TaxNumber { get; set; }

    ...
}

[DatabaseClass]
[HashIndex(NameIndex, false, nameof(FirstName), nameof(LastName))]
public abstract class Person : Entity
{
    public const string NameIndex = "PersonNameIndex";

    [DatabaseProperty]
    public abstract string FirstName { get; set; }

    [DatabaseProperty]
    public abstract string LastName { get; set; }

    ...
}

[DatabaseClass]
[HashIndex(NameIndex, true, nameof(Name))]
public abstract class Product : DatabaseObject
{
    public const string NameIndex = "ProductNameIndex";

    [DatabaseProperty]
    public abstract string Name { get; set; }

    ...
}
```

We are enforcing a unique user name for any entity in the database, which means that no two legal entities and persons will be able to share the same username. If, for some reason, you wanted to enforce uniqueness for legal entities and persons separately, you would need to define two indexes, one for legal entities and one for persons.

>[!NOTE]
>We recommend that you name your hash indexes using constant strings inside the indexed classes. This way, consuming the indexes (described in the [Database APIs](database_apis.md) chapter) will be easier.

>[!CAUTION]
>Hash indexes are best suited for unique keys. Having duplicate keys in the hash index increases the length of collision chains and makes searching and updating the index slower. Generally speaking, having more than a dozen objects per single key is not recommended. It is best to test your specific scenario to see if the performance is satisfactory.

### Estimating Hash Index Memory Requirements

Estimating the memory requirements of a hash index is relatively easy. Given the rate of change of indexed classes, changeRate, and total number of indexed objects, indexedObjCount total memory size is given with the following formula:

```
size = indexedObjCount * 24 + changeRate * maxTranDuration * 16
```

## Model Deployment

Deploying your data model to the database instance is easy (we are not talking here about development deployment discussed in the [Getting Started](getting_started.md) chapter). Once you build your data model and produce one or more .NET assemblies, copy them into a separate directory. As discussed previously, all assemblies that are deployed to the database need to be copied to this directory because the vlx tool uploads all assemblies at once and does not offer a way to only upload a single assembly or a subset of assemblies. Once this directory has been prepared, run the following CLI commands (in direct or interactive mode):

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

You will be presented with a list of changes that were detected, compared to the current state in the database, and prompted to confirm the action. the output might look something like this:

```accesslog
orders_model.dll      Inserted
Do you want to proceed (Y/N)?Y
```
