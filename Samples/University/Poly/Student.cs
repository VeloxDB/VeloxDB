using Velox.ObjectInterface;

namespace University;

[DatabaseClass]
public abstract partial class Student : Person
{
	[DatabaseReference]
	public abstract ReferenceArray<Course> Courses { get; set; }

	[DatabaseProperty]
	public abstract DatabaseArray<int> Scores { get; set; }

	public new partial StudentDTO ToDTO();
	public static partial Student FromDTO(ObjectModel om, StudentDTO dto);
}

#region StudentDTO
public class StudentDTO : PersonDTO
{
	public List<CourseDTO>? Courses { get; set; }
	public int[]? Scores { get; set; }
}
#endregion
