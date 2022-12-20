using VeloxDB.ObjectInterface;

namespace University;

[DatabaseClass(true)]
public abstract partial class Person : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Name { get; set; }

	[DatabaseProperty]
	public abstract string Address { get; set; }
}

public class PersonDTO
{
	public string? Name { get; set; }
	public string? Address { get; set; }
}
