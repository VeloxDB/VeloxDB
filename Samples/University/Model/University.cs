using VeloxDB.ObjectInterface;

namespace University;

[DatabaseClass(true)]
public abstract class Person : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Name { get; set; }

	[DatabaseProperty]
	public abstract string Address { get; set; }
}

[DatabaseClass]
public abstract class Student : Person
{
	[DatabaseReference]
	public abstract ReferenceArray<Course> Courses { get; set; }

	[DatabaseProperty]
	public abstract DatabaseArray<int> Scores { get; set; }
}

[DatabaseClass]
public abstract class Teacher : Person
{
	[DatabaseReference]
	public abstract Student? Assistant { get; set; }

	[DatabaseReference]
	public abstract ReferenceArray<Course> Teaches { get; set; }
}

[DatabaseClass]
public abstract class Course : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Name { get; set; }
}
