using System;
using System.Reflection;

namespace VeloxDB.Client;

internal abstract class ConnectionBase
{
	public static readonly ConstructorInfo ConstructorMethod = typeof(ConnectionBase).GetConstructor(
		BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly, new Type[] { typeof(string) });

	public static readonly FieldInfo ConnStrField = typeof(ConnectionBase).GetField(nameof(connectionString),
		BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly);

	protected string connectionString;

	internal ConnectionBase(string connectionString)
	{
		this.connectionString = connectionString;
	}

	public string ConnectionString => connectionString;
}
