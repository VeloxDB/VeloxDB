---
uid: Velox.ObjectInterface.ReferenceArray`1
example:
  - 'The following example demonstrates how to declare a <xref:Velox.ObjectInterface.ReferenceArray`1> property.'
  - '[!code-csharp[Main](../../Samples/ModelExamples/WeatherStation.cs#City)]'
  - 'The following example demonstrates how to use a <xref:Velox.ObjectInterface.ReferenceArray`1>.'
  - '[!code-csharp[Main](../../Samples/ModelExamples/WeatherStation.cs#CreateTestCity)]'
remarks: *content
---
VeloxDB allows you to specify an array like collection. <xref:Velox.ObjectInterface.ReferenceArray`1> can only hold references to <xref:Velox.ObjectInterface.DatabaseObject>, if you need an array of references to simple types, use <xref:Velox.ObjectInterface.DatabaseArray`1>. <xref:Velox.ObjectInterface.ReferenceArray`1> property must be marked with <xref:Velox.ObjectInterface.DatabaseReferenceAttribute> attribute because it represents a reference to another <xref:Velox.ObjectInterface.DatabaseObject>.

<xref:Velox.ObjectInterface.ReferenceArray`1> is backed by an array. Array's size is initially set to capacity. As long as the capacity is larger than the length of the array Add is constant time operation (O(1)). When there is no more space in the backing array new array is allocated. The new array is twice the size of the previous one. Contents of the old array are copied to the new array. This gives <xref:Velox.ObjectInterface.ReferenceArray`1> amortized constant time adds, constant time direct access and linear remove.


