---
uid: Velox.ObjectInterface.ObjectModel
remarks: *content
---
<xref:Velox.ObjectInterface.ObjectModel> provides interface for working with the database. You can use it to fetch objects, create new objects and query indexes. The data provided by the <xref:Velox.ObjectInterface.ObjectModel> always reflects changes made during the transaction. For example, if you were to create a new object, and then iterate through all objects you would find your newly created object.

For references `IsNullable` is not enforced during the transaction. That is VeloxDB will allow allow reference with `IsNullable` set to `false` to be null during the execution of the database operation. This is done intentionally because sometimes you need a model to be temporarily in invalid state, for example that's the only way to create a circular reference. This check is enforced at the end of database operation, if there is reference that is not nullable with null value at the end of operation, database will rollback transaction and report an error.

VeloxDB stores data in an unmanaged memory. <xref:Velox.ObjectInterface.DatabaseObject> is just a thin wrapper around this unmanaged memory structure. When you access an object, the database allocates new <xref:Velox.ObjectInterface.DatabaseObject> and keeps it around for the duration of the transaction. These objects are also used to store changes to the database done during the transaction. This enables VeloxDB to always provide you with the same object no matter how you access it. This also has certain drawbacks, for example when iterating through large amounts of objects (either using <xref:Velox.ObjectInterface.ObjectModel.GetAllObjects``1> or by following references) you can end up allocating a lot of objects that VeloxDB will keep around until the transaction is done. If you know that you wont be applying changes to these objects, you can call <xref:Velox.ObjectInterface.DatabaseObject.Abandon> on them. This tells VeloxDB that it doesn't have to keep the reference to the object any more.

This example illustrates how to use <xref:Velox.ObjectInterface.DatabaseObject.Abandon> method.
[!code-csharp[Main](../../Samples/ModelExamples/WeatherStation.cs#CountNewCities)]

Changes done to the objects are kept within <xref:Velox.ObjectInterface.ObjectModel>. In case of large transactions, these changes can accumulate and negatively affect performance. You can use <xref:Velox.ObjectInterface.ObjectModel.ApplyChanges> method to apply changes to the database and clear <xref:Velox.ObjectInterface.ObjectModel>'s internal cache's.