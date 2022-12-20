---
uid: VeloxDB.ObjectInterface.DatabaseArray`1
example:
  - 'The following example demonstrates how to declare a <xref:VeloxDB.ObjectInterface.DatabaseArray`1> property.'
  - '[!code-csharp[Main](../../Samples/ModelExamples/WeatherStation.cs#WeatherStation)]'
  - 'The following example demonstrates how to use a <xref:VeloxDB.ObjectInterface.DatabaseArray`1>.'
  - '[!code-csharp[Main](../../Samples/ModelExamples/WeatherStation.cs#CreateTestStation)]'
remarks: *content
---
VeloxDB allows you to specify an array like collection. <xref:VeloxDB.ObjectInterface.DatabaseArray`1> can only hold simple types, if you need an array of references to other [DatabaseObjects](xref:VeloxDB.ObjectInterface.DatabaseObject), use <xref:VeloxDB.ObjectInterface.ReferenceArray`1>.

<xref:VeloxDB.ObjectInterface.DatabaseArray`1> is backed by an array. Array's size is initially set to capacity. As long as the capacity is larger than the length of the array Add is constant time operation (O(1)). When there is no more space in the backing array new array is allocated. The new array is twice the size of the previous one. Contents of the old array are copied to the new array. This gives <xref:VeloxDB.ObjectInterface.DatabaseArray`1> amortized constant time adds, constant time direct access and linear remove.


