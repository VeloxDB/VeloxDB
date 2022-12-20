---
uid: VeloxDB.ObjectInterface.DatabasePropertyAttribute
example:
  - '[!code-csharp[Main](../../Samples/Performance/CRUDPerfSample/Server/Vehicle.cs#Vehicle)]'
remarks: *content
---

Apply the [](xref:VeloxDB.ObjectInterface.DatabasePropertyAttribute) to properties of [](xref:VeloxDB.ObjectInterface.DatabaseObject) that need to be persisted. Database properties must be abstract, with both getter and setter defined. 

Database properties can be of following types:
* `byte`
* `short`
* `int`
* `long`
* `float`
* `double`
* `bool`
* `System.DateTime`
* `string`
* [](xref:VeloxDB.ObjectInterface.DatabaseArray`1)

> [!NOTE]
> Default values are not supported for [](xref:VeloxDB.ObjectInterface.DatabaseArray`1) and string types.