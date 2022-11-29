---
uid: Velox.ObjectInterface.DatabaseClassAttribute
example:
  - '[!code-csharp[Main](../../Samples/GetStarted/VlxBlog/Model.cs#Post)]'
remarks: *content
---

Apply the [](xref:Velox.ObjectInterface.DatabaseClassAttribute) to classes that are part of the model.
A database class must fulfill the following requirements:
* It must be declared as abstract
* It must inherit from [](xref:Velox.ObjectInterface.DatabaseObject)
* It must have an empty constructor
* Properties that need to be persisted must be marked with [](xref:Velox.ObjectInterface.DatabasePropertyAttribute)
* Persisted properties must be abstract with both getter and setter defined

