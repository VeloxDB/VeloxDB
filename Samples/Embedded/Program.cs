using VeloxDB.Embedded;
using VeloxDB.ObjectInterface;

namespace VeloxDB.Samples.Embedded;

[DatabaseClass]
public abstract class Message : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Content {get; set;}
}

internal class Program
{
    private static void Main(string[] args)
    {
		using VeloxDBEmbedded db = new VeloxDBEmbedded("/home/div0/Development/temp/vlx", [typeof(Message).Assembly], false);

		using (VeloxDBTransaction trans = db.BeginTransaction())
		{
			Message message = trans.ObjectModel.CreateObject<Message>();
			message.Content = "Hello embedded world!";
			trans.Commit();
		}

		using (VeloxDBTransaction trans = db.BeginTransaction(TransactionType.Read))
		{
			foreach (var m in trans.ObjectModel.GetAllObjects<Message>())
			{
				Console.WriteLine(m.Content);
			}
		}
    }
}