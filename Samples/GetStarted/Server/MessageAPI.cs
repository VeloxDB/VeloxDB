using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Server;

[DbAPI(Name = "MessageAPI")]
public class MessageAPI
{
	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	public string[] GetAllMessages(ObjectModel om)
	{
		return om.GetAllObjects<Message>().Select(m => m.Text).ToArray();
	}

	[DbAPIOperation]
	public void AddMessage(ObjectModel om, string text)
	{
		Message message = om.CreateObject<Message>();
		message.Text = text;
	}
}