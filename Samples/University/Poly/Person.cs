using Velox.ObjectInterface;

namespace University;

[DatabaseClass(true)]
public abstract partial class Person : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Name { get; set; }

	[DatabaseProperty]
	public abstract string Address { get; set; }

	[SupportPolymorphism]
	public partial PersonDTO ToDTO();

	[SupportPolymorphism]
	public static partial Person FromDTO(ObjectModel om, PersonDTO dto);
}

public class PersonDTO
{
	public string? Name { get; set; }
	public string? Address { get; set; }
}
