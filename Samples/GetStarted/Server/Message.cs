using VeloxDB.ObjectInterface;

namespace Server;

[DatabaseClass]
public abstract class Message : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Text { get; set; }

}
