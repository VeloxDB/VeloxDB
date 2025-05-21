using VeloxDB.Client;

ConnectionStringParams csp = new ConnectionStringParams();
csp.AddAddress("localhost:7568");
csp.RetryTimeout = 20000;
csp.OpenTimeout = 20000;

IMessageAPI messageAPI = ConnectionFactory.Get<IMessageAPI>(csp.GenerateConnectionString());
await messageAPI.AddMessage("Hello, world!");

foreach (var message in await messageAPI.GetAllMessages())
{
	Console.WriteLine(message);
}