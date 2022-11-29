using System;
using Velox.Protocol;

namespace Velox.Server;
[DbAPI(Name = AdminAPIServiceNames.LocalWriteClusterAdministration)]
public interface ILocalWriteClusterAdministration
{
    Task Failover();
    Task BecomePrimarySite();
    Task BecomeStandbySite();
}
