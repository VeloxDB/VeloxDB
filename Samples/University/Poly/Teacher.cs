using VeloxDB.ObjectInterface;

namespace University;

#region Teacher
[DatabaseClass]
public abstract partial class Teacher : Person
{
	[DatabaseReference]
	public abstract Student? Assistant { get; set; }

	[DatabaseReference]
	public abstract ReferenceArray<Course> Teaches { get; set; }

	public new partial TeacherDTO ToDTO();
	public static partial Teacher FromDTO(ObjectModel om, TeacherDTO dto);
}

#endregion
