using VeloxDB.Client;
using VeloxDB.Protocol;

[DbAPI(Name = "MessageAPI")]
public interface IMessageAPI
{
	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	DatabaseTask<string[]> GetAllMessages();
	[DbAPIOperation]
	DatabaseTask AddMessage(string text);
}