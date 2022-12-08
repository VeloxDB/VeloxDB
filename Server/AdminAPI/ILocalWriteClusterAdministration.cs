using System;
using Velox.Client;
using Velox.Protocol;

namespace Velox.Server;
[DbAPI(Name = AdminAPIServiceNames.LocalWriteClusterAdministration)]
public interface ILocalWriteClusterAdministration
{
	DatabaseTask Failover();
	DatabaseTask BecomePrimarySite();
	DatabaseTask BecomeStandbySite();
}
