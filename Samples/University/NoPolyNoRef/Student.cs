using Velox.ObjectInterface;

namespace University;

[DatabaseClass]
public abstract partial class Student : Person
{
	[DatabaseReference]
	public abstract ReferenceArray<Course> Courses { get; set; }

	[DatabaseProperty]
	public abstract DatabaseArray<int> Scores { get; set; }

	public partial StudentDTO ToDTO();
	public static partial Student FromDTO(ObjectModel om, StudentDTO dto);
}

#region StudentDTO
public class StudentDTO
{
	public string? Name { get; set; }
	public string? Address { get; set; }
	public List<long>? CoursesIds { get; set; }
	public int[]? Scores { get; set; }
}
#endregion
