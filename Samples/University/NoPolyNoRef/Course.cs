using Velox.ObjectInterface;

namespace University;

[DatabaseClass]
public abstract partial class Course : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Name { get; set; }

	public partial CourseDTO ToDTO();

	public static partial Course FromDTO(ObjectModel om, CourseDTO dto);
}

#region CourseDTO
public class CourseDTO
{
	public long Id { get; set; }
	public string? Name { get; set; }
}
#endregion
