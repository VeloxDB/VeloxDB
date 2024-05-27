using System;
using System.Xml.Linq;
using VeloxDB.Client;
using VeloxDB.Config;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Modes;

internal sealed class InitialMode : Mode
{
	string clusterConfigFileName;
	ClusterConfiguration clusterConfig;

	public InitialMode()
	{
	}

	public override string Title => "vlx";
	public override Mode Parent => null;

	public bool UseSSL { get; private set; }
	public bool IgnoreCertificate { get; set; }
	public string CACertificate { get; private set; }
	public string[] Certificates { get; private set; }

	public string ClusterConfigFileName => clusterConfigFileName;

	internal ClusterConfiguration ClusterConfig => clusterConfig;

	public void SetClusterConfiguration(string clusterConfigFileName, ClusterConfiguration clusterConfig)
	{
		this.clusterConfigFileName = clusterConfigFileName;
		this.clusterConfig = clusterConfig;
	}

	private void CollectAddresses(ReplicationElement nodeOrCluster, List<string> addresses)
	{
		if (nodeOrCluster.Type == ElementType.LocalWrite)
		{
			LocalWriteCluster lwCluster = (LocalWriteCluster)nodeOrCluster;
			if (lwCluster.First != null)
				addresses.Add(lwCluster.First.AdministrationAddress.ToString());

			if (lwCluster.Second != null)
				addresses.Add(lwCluster.Second.AdministrationAddress.ToString());
		}
		else
		{
			addresses.Add((nodeOrCluster as StandaloneNode).AdministrationAddress.ToString());
		}
	}

	public string GetDatabaseConnectionString()
	{
		if (clusterConfig == null || clusterConfig.Cluster == null)
			throw new InvalidOperationException();

		List<string> address = new List<string>(4);
		if (clusterConfig.Cluster.Type == ElementType.GlobalWrite)
		{
			GlobalWriteCluster gwCluster = (GlobalWriteCluster)clusterConfig.Cluster;
			if (gwCluster.First != null)
				CollectAddresses(gwCluster.First, address);

			if (gwCluster.Second != null)
				CollectAddresses(gwCluster.Second, address);
		}
		else
		{
			CollectAddresses(clusterConfig.Cluster, address);
		}

		ConnectionStringParams cp = new ConnectionStringParams();
		address.ForEach(x => cp.AddAddress(x));
		cp.ServiceName = AdminAPIServiceNames.DatabaseAdministration;
		cp.RetryTimeout = Program.ConnectionRetryTimeout;
		cp.OpenTimeout = Program.ConnectionOpenTimeout;
		cp.VerifyCert = !IgnoreCertificate;
		cp.UseSSL = IgnoreCertificate;
		cp.PoolSize = 1;
		SetCSP(cp);

		return cp.GenerateConnectionString();
	}

	public void SetSSLConfiguration(bool useSSL, bool ignoreCertificate, string caCertificate, string[] certificates)
	{
		UseSSL = useSSL;
		IgnoreCertificate = ignoreCertificate;
		CACertificate = caCertificate;
		Certificates = certificates;
	}

	public void SetCSP(ConnectionStringParams csp)
	{
		csp.UseSSL = UseSSL;
		csp.VerifyCert = !IgnoreCertificate;
		csp.CACert = CACertificate;

		foreach (string cert in Certificates)
			csp.AddServerCert(cert);
	}
}
