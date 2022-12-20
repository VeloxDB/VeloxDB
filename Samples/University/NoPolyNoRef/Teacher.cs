using VeloxDB.ObjectInterface;

namespace University;

[DatabaseClass]
public abstract partial class Teacher : Person
{
	[DatabaseReference]
	public abstract Student? Assistant { get; set; }

	[DatabaseReference]
	public abstract ReferenceArray<Course> Teaches { get; set; }

	public partial TeacherDTO ToDTO();
	public static partial Teacher FromDTO(ObjectModel om, TeacherDTO dto);
}

#region TeacherDTO
public class TeacherDTO
{
	public string? Name { get; set; }
	public string? Address { get; set; }
	public long AssistantId { get; set; }
	public long[]? TeachesIds { get; set; }
}
#endregion

