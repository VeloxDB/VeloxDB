# Todo
[*] When dto has an array of object references, and it contains null, error thrown should probably be DBE and it should be consistent in all such cases (like if Id is 0)
[*] Make it so that by default FromDTO doesn't update
[*] When validating references, ToDTO method needs to be accessible to the referencing class (Best place to check this is CheckDTO method)
[*] FromDTO overload protection (because both 2 and 3 arg variant is ok)
[*] Wrong type for DTO in FromDTO generates wrong error message, the error message implies that return value is wrong when it's actually argument that is wrong
[*] Add validation preventing the same property to be mentioned 2 times in DTO
[] Add Remarks to SupportPolymorphismAttribute documentation
[] Add guide for automapper


[*] Class initialization should be moved from static constructors to something from server - It actually doesn't have to, visiting all database classes with reflection handles this.
[] Include generated source in pdbs?
[] Looks like Select() pattern is not the simplest scenario, and maybe the default should be something simpler... 
[] Add DTO generation analyzer
[] Move syntax checks to analyzer so errors could be generated in devtime... 
[] struct support?

Featuer-i
References over Ids/objects
Arrays(ref/noref)
Update!
Circular graph detection


|From/To  |Polymorphism/No Polymorph| References       |   Test        |
|---------|-------------------------|------------------|---------------|
| From    |           0             |    NoRef         |TestSimpleType |
| From    |           0             |    NoRefArray    |TestSimpleArray|
| From    |           0             |    OverId        |TestReference  |
| From    |           0             |    OverIdArray   |TestReferenceArrays|
| From    |           0             |    OverObj       |TestObjectReference, TestSameNode|
| From    |           0             |    OverObjArray  |TestObjRefArrays|
| From    |           1             |    NoRef         |TestPolymorphismSupport|
| From    |           1             |    NoRefArray    |ComplexTest    |
| From    |           1             |    OverId        |ComplexTest    |
| From    |           1             |    OverIdArray   |ComplexTest    |
| From    |           1             |    OverObj       |ComplexTest    |
| From    |           1             |    OverObjArray  |ComplexTest    |
| Update  |           0             |    NoRef         |TestUpdate     |
| Update  |           0             |    NoRefArray    |               |
| Update  |           0             |    OverId        |               |
| Update  |           0             |    OverIdArray   |               |
| Update  |           0             |    OverObj       |TestUpdate     |
| Update  |           0             |    OverObjArray  |               |
| Update  |           1             |    NoRef         | 			   |
| Update  |           1             |    NoRefArray    | 			   |
| Update  |           1             |    OverId        |               |
| Update  |           1             |    OverIdArray   |               |
| Update  |           1             |    OverObj       |               |
| Update  |           1             |    OverObjArray  |               |
|   To    |           0             |    NoRef         |TestSimpleType, TestUpdate |
|   To    |           0             |    NoRefArray    |TestSimpleArray|
|   To    |           0             |    OverId        |TestReference  |
|   To    |           0             |    OverIdArray   |TestReferenceArrays|
|   To    |           0             |    OverObj       |TestObjectReference, TestSameNode, TestUpdate|
|   To    |           0             |    OverObjArray  |TestObjRefArrays|
|   To    |           1             |    NoRef         |TestPolymorphismSupport|
|   To    |           1             |    NoRefArray    |ComplexTest|
|   To    |           1             |    OverId        |ComplexTest|
|   To    |           1             |    OverIdArray   |ComplexTest|
|   To    |           1             |    OverObj       |ComplexTest|
|   To    |           1             |    OverObjArray  |ComplexTest|