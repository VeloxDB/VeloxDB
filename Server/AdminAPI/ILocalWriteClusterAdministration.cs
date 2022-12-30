using System;
using VeloxDB.Client;
using VeloxDB.Protocol;

namespace VeloxDB.Server;
[DbAPI(Name = AdminAPIServiceNames.LocalWriteClusterAdministration)]
public interface ILocalWriteClusterAdministration
{
	DatabaseTask Failover();
	DatabaseTask BecomePrimarySite();
	DatabaseTask BecomeStandbySite();
}
