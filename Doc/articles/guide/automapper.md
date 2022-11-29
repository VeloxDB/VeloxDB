---
uid: automapper
---

# Automapper

Since moving data between DTOs and DBOs is a common operation, VeloxDB comes bundled with automapper. Automapper automatically generates methods that copy data between DTO and DBO saving you from typing boilerplate code. The use of automapper is optional, you can write your own object copying code, or use 3rd party automapper like [AutoMapper][1] or [Mapster][2].

Here is a brief overview of automapper features:
* Compile time - Automapper is built using C#'s [source generator][3] feature. As such it runs during compile time and mistakes are reported as compiler errors. This also has an added benefit of providing compile time validation for VeloxDB model.
* Built with VeloxDB in mind - It is aware that it is working with database objects and provides various features that are harder to achieve with generic automappers.
* Fast - Automapper is generated at compile time, it doesn't use reflection. Because of this it is as fast as if it was hand written.
* Support for references - Referenced objects can be mapped either as objects or as Ids.
* Support for circular references
* Support for deep reference graphs - When copying object graphs recursively there's a risk of encountering stack overflow if the object graph is too deep, VeloxDB automapper uses algorithm that doesn't have this problem.
* Support for polymorphism

Throughout this guide, we will be using the following model:

[!code-csharp[Main](../../../Samples/University/Model/University.cs)]

## Simple mapping

The core feature of the automapper is mapping properties from one object to another. VeloxDB automapper supports mapping properties from database object to DTO and from DTO to database object. Lets consider a simple DTO class for `Course` class:

[!code-csharp[Main](../../../Samples/University/NoPolyNoRef/Course.cs#CourseDTO)]

The DTO contains two properties `Name` and `Id`. In order to map `Course` class to its DTO we will need to add automapper method to it. Automapper methods are partial methods added to database classes. Their body is filled in by the automapper source generator during compile time. Automapper methods that copy data to DTO must start with `To` prefix. Here is an example of method that creates `CourseDTO` from `Course` object.

```cs
public partial CourseDTO ToDTO();
```

Automapper maps properties by name and type. If DTO class has propertIes that don't map to any database property, automapper will report a warning. On the other hand DTO doesn't need to have all database class's properties.

To create a method that creates database object from DTO, create a static partial method in the database class that starts with `From`. Here is an example:

```cs
public static partial Course FromDTO(ObjectModel om, CourseDTO dto);
```

Note that the method takes <xref:Velox.ObjectInterface.ObjectModel> as an argument. This is because it needs ObjectModel in order to create a new object in the database. In this case the id property is ignored and a new object is always created, if you need update functionality see[update](#update) part of this guide.

## Mapping arrays

Automapper supports mapping arrays. It has full support for both arrays of simple types and reference arrays. VeloxDB represents arrays with either <xref:Velox.ObjectInterface.DatabaseArray> or <xref:Velox.ObjectInterface.ReferenceArray>. These classes are not available on client side and should be mapped to either an array or [List][4]. Here is an example of `StudentDTO` DTO class that includes arrays:
[!code-csharp[Main](../../../Samples/University/NoPoly/Student.cs#StudentDTO)]

If the value of array in database object is null, automapper will also produce null in the mapped object.

## Mapping references

There are two ways to map a reference property, by mapping by id, or by mapping to DTO.

The simplest approach is to map a reference by id. This is done by creating a DTO property of the same name and `long` type. The result is that referenced object's id is written into the property. When naming the DTO property in this case it is also valid to add Id or Ids suffix. Id mapping also works for arrays by mapping to `long[]` or `List<long>` types. Here is an example of `TeacherDTO` class that uses Id mapping:

[!code-csharp[Main](../../../Samples/University/NoPolyNoRef/Teacher.cs#TeacherDTO)]

You can note `AssistantId` property which maps to `Assistant` property in `Teacher` class, and `TeachesIds` property which maps to `Teaches` property. If the reference points to `null`, 0 will be written to Id property. When creating database object from DTO, these ids are used to fetch objects from the database and assign them to database object. In case the object does not exist in the database, `From` method will throw an [ArgumentException][5].

Besides mapping to id, reference property can be mapped to another DTO. This enables you to easily map object graphs. In order to map a reference property to another DTO, referenced object must also contain To/From method with the same name. Here is an example of DTO with reference to another DTO:

[!code-csharp[Main](../../../Samples/University/NoPoly/Teacher.cs#TeacherDTO)]

In order for this mapping to work `Course` database class must also have mapping methods with same names which map `Course` to `CourseDTO`. Here is an example of `Course` class and its DTO:

[!code-csharp[Main](../../../Samples/University/NoPoly/Course.cs)]

Mapping object graphs introduces some problems. For example if you have graph like, deeply connected model, you could easily end up mapping the whole database and sending it to the client. Since this behavior is not desired, VeloxDB automapper provides a method for selecting which objects should be mapped. By default, only the object you called To method on will be mapped, objects it references will not be mapped. In order to map referenced objects to DTOs you need to call <xref:Velox.ObjectInterface.DatabaseObject.Select> method on them first.
Here is an example of database operation that maps `Teacher` and all referenced courses to DTO:

[!code-csharp[Main](../../../Samples/University/NoPoly/UniApi.cs#GetTeacher)]

It is also possible to map inverse references to DTOs. They are treated in the same way as reference arrays. They can be mapped to either ids or objects. Here is an example of `Course` class with inverse reference and its DTO:

[!code-csharp[Main](../../../Samples/University/InvRef/Course.cs)]

>[!CAUTION]
>It is strongly advised against using a reference in both directions (direct and inverse) in DTOs. If the DTO is not consistent, for example object A points to object B but inverse reference in object B does not point back to A, mapping to database object can produce unexpected results. Mapping from database object to DTO does not have these problems.

VeloxDB automapper is aware of object identity, if two objects point to the same object, the same will be true after mapping, it will not create two objects. It is also capable of handling circular references.

## Polymorphism

## Update

## Debugging

[1]: https://automapper.org/
[2]: https://github.com/MapsterMapper/Mapster
[3]: https://learn.microsoft.com/en-us/dotnet/csharp/roslyn-sdk/source-generators-overview
[4]: https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1
[5]: https://learn.microsoft.com/en-us/dotnet/api/system.argumentexception