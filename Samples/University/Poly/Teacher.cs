using Velox.ObjectInterface;

namespace University;

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

#region TeacherDTO
public class TeacherDTO : PersonDTO
{
	public long AssistantId { get; set; }
	public CourseDTO[]? TeachesIds { get; set; }
}
#endregion

