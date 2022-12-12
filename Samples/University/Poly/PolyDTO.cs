using Velox.ObjectInterface;

namespace University;

#region PolyDTO
public class PersonDTO
{
	public string? Name { get; set; }
	public string? Address { get; set; }
}

public class StudentDTO : PersonDTO
{
	public List<CourseDTO>? Courses { get; set; }
	public int[]? Scores { get; set; }
}

public class TeacherDTO : PersonDTO
{
	public long AssistantId { get; set; }
	public CourseDTO[]? TeachesIds { get; set; }
}
#endregion
