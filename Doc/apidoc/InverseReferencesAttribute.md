---
uid: Velox.ObjectInterface.InverseReferencesAttribute
example:
  - 'The following example demonstrates how to declare an <xref:Velox.ObjectInterface.InverseReferencesAttribute> property.'
  - '[!code-csharp[Main](../../Samples/ModelExamples/Blog.cs#Blog)]'
  - 'The following example demonstrates how to use an inverse reference.'
  - '[!code-csharp[Main](../../Samples/ModelExamples/Blog.cs#TestBlog)]'
remarks: *content
---
When you define a reference between two classes using <xref:Velox.ObjectInterface.DatabaseReferenceAttribute>, you can also define an inverse reference on the target class. Inverse reference enables you to navigate a reference in reverse direction. To declare an inverse reference declare a property of <xref:Velox.ObjectInterface.InverseReferenceSet`1> type and mark it with <xref:Velox.ObjectInterface.InverseReferencesAttribute>.

> [!NOTE]
> <xref:Velox.ObjectInterface.DatabaseReferenceAttribute.TrackInverseReferences> must be set to true for the reference if you want to use inverse references.