---
uid: VeloxDB.ObjectInterface.DatabaseClassAttribute
example:
  - '[!code-csharp[Main](../../Samples/SimpleExample/VlxBlog/Model.cs#Post)]'
remarks: *content
---

Apply the [](xref:VeloxDB.ObjectInterface.DatabaseClassAttribute) to classes that are part of the model.
A database class must fulfill the following requirements:
* It must be declared as abstract
* It must inherit from [](xref:VeloxDB.ObjectInterface.DatabaseObject)
* It must have an empty constructor
* Properties that need to be persisted must be marked with [](xref:VeloxDB.ObjectInterface.DatabasePropertyAttribute)
* Persisted properties must be abstract with both getter and setter defined

