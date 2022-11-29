using Velox.Protocol;
using Velox.Storage;
using Velox.Storage.Replication.HighAvailability;

namespace Velox.Server;

[DbAPI(Name = AdminAPIServiceNames.LocalWriteClusterAdministration)]
public sealed class LocalWriteClusterAdministration
{
	private IElector? lwElector;
	private IElector? gwElector;
	private StorageEngine engine;

	internal LocalWriteClusterAdministration(IElector? lwElector, IElector? gwElector, StorageEngine engine)
	{
		this.lwElector = lwElector;
		this.gwElector = gwElector;
		this.engine = engine;
	}

	[DbAPIOperation]
	public void BecomePrimarySite()
	{
		if (gwElector == null)
			throw DbExc(DatabaseErrorType.NotInGlobalWriteCluster);

		ElectorResponse result = gwElector.BecomePrimary(0);
		CheckResult(result);
	}

	private static DatabaseException DbExc(DatabaseErrorType errorType)
	{
		return new DatabaseException(DatabaseErrorDetail.Create(errorType));
	}

	[DbAPIOperation]
	public void BecomeStandbySite()
	{
		if(gwElector == null)
			throw DbExc(DatabaseErrorType.NotInGlobalWriteCluster);

		ElectorResponse result = gwElector.BecomeStandby();
		CheckResult(result);
	}

	[DbAPIOperation]
	public void Failover()
	{
		if(lwElector == null)
			throw DbExc(DatabaseErrorType.NotInLocalWriteCluster);

		ElectorResponse result = lwElector.TryFailover();
		CheckResult(result);
	}

	private static void CheckResult(ElectorResponse result)
	{
		if (result == ElectorResponse.Success)
			return;

		if (result == ElectorResponse.NotApplicable)
			throw DbExc(DatabaseErrorType.NotApplicable);

		if (result == ElectorResponse.Busy)
			throw DbExc(DatabaseErrorType.DatabaseBusy);
	}

}
