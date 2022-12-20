using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace University;

[DbAPI]
public class UniversityAPI
{
	#region GetTeacher
	public TeacherDTO? GetTeacher(ObjectModel om, long id)
	{
		Teacher? teacher = om.GetObject<Teacher>(id);
		if(teacher == null)
			return null;

		foreach(Course course in teacher.Teaches)
		{
			course.Select();
		}

		return teacher.ToDTO();
	}
	#endregion
}
