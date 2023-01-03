---
uid: VeloxDB.ObjectInterface.SupportPolymorphismAttribute
example:
  - 'The following example demonstrates how to declare an automapper method that supports polymorphism.'
  - '[!code-csharp[Main](../../Samples/University/Poly/Person.cs)]'
  - 'And here you can see how to specify automapper methods in derived class.'
  - '[!code-csharp[Main](../../Samples/University/Poly/Student.cs)]'
remarks: *content
---
AutoMapper methods by default don't support polymorphism. This is because polymorphism support introduces slight performance overhead. In order to support polymorphism, mark the automapper methods with <xref:VeloxDB.ObjectInterface.SupportPolymorphismAttribute>. If you don't mark the base methods with <xref:VeloxDB.ObjectInterface.SupportPolymorphismAttribute> automapper generator will prevent you from extending the base class. The derived class must contain all automapper methods that the base class contains. DTO Type should be the DTO Type of the derived class, not base class. Automapper uses this information in order to determine which database type maps to which DTO. For more detailed overview of automapper see <xref:database_apis#automapper> section of the guide.