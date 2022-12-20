using VeloxDB.ObjectInterface;

namespace University;

[DatabaseClass]
public abstract partial class Course : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Name { get; set; }

	[InverseReferences(nameof(Teacher.Teaches))]
	public abstract InverseReferenceSet<Teacher> TaughtBy { get; }

	public partial CourseDTO ToDTO();
	public static partial Course FromDTO(ObjectModel om, CourseDTO dto);
}

public class CourseDTO
{
	public long Id { get; set; }
	public string? Name { get; set; }
	public long[]? TaughtByIds { get; set; }
}

