---
uid: VeloxDB.ObjectInterface.HashIndexAttribute
example:
  - The following example demonstrates how to create a hash index.
  - '[!code-csharp[Main](../../Samples/ModelExamples/WeatherStation.cs#City)]'
  - The following example demonstrates how to use a hash index.
  - '[!code-csharp[Main](../../Samples/ModelExamples/WeatherStation.cs#GetCityTempByName)]'
remarks: *content
---
You can use <xref:VeloxDB.ObjectInterface.HashIndexAttribute> to create a hash index. Hash index allows you to quickly lookup an object using its properties. After you declare an index with <xref:VeloxDB.ObjectInterface.HashIndexAttribute> use <xref:VeloxDB.ObjectInterface.ObjectModel.GetHashIndex``2(System.String)> to fetch a <xref:VeloxDB.ObjectInterface.HashIndexReader`2> which you can use for object lookup.

Hash index can also be used to enforce uniqueness constraint on a property. Use <xref:VeloxDB.ObjectInterface.IndexAttribute.IsUnique> to declare that index must be unique.

Support for composite keys is also available, you can specify up to 4 properties to be included in hash index's key.

> [!NOTE]
> Hash index defined on a base class will also include all subclasses as well.