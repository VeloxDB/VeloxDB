using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal class SslClientOptionsFactory
{
    readonly CertificateMap certificateMap;
    readonly bool verifyCert;
    readonly X509Certificate2 caCert;

    public SslClientOptionsFactory(CertificateMap certificateMap, bool verifyCert, X509Certificate2 caCert)
    {
		this.certificateMap = certificateMap;
        this.verifyCert = verifyCert;
		this.caCert = caCert;
    }

    public static SslClientOptionsFactory Create(X509Certificate2[] serverCerts, bool verifyCert, string caCertPath)
    {
        var certificateMap = new CertificateMap(serverCerts);
		return Create(certificateMap, verifyCert, caCertPath);
    }

    public static SslClientOptionsFactory Create(string[] serverCertPaths, bool verifyCert, string caCertPath)
    {
        var certificateMap = CertificateMap.Create(serverCertPaths);
		return Create(certificateMap, verifyCert, caCertPath);
    }

    private static SslClientOptionsFactory Create(CertificateMap certificateMap, bool verifyCert, string caCertPath)
    {
		X509Certificate2 caCert = null;
		if (!string.IsNullOrEmpty(caCertPath))
 		{
			caCert = SSLUtils.LoadCertificate(caCertPath, onlyPublicKey: true);
		}

		return new SslClientOptionsFactory(certificateMap, verifyCert, caCert);
    }


    public SslClientAuthenticationOptions CreateSslOptions(string host)
    {
        SslClientAuthenticationOptions sslOptions = new();

        if (!verifyCert)
        {
            sslOptions.RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true;
        }
        else if (certificateMap.Count > 0)
        {
            X509Certificate2 serverCert = certificateMap.GetCert(host);
            if (serverCert != null)
                sslOptions.RemoteCertificateValidationCallback = (sender, cert, chain, errors) =>
				Utils.ByteArrayEqual(serverCert.GetPublicKey(), cert.GetPublicKey());
        }

		if (caCert != null)
		{
			sslOptions.CertificateChainPolicy = new X509ChainPolicy
			{
				TrustMode = X509ChainTrustMode.CustomRootTrust,
				CustomTrustStore = { caCert },
				RevocationMode = X509RevocationMode.NoCheck
			};
		}

        sslOptions.TargetHost = host;

        return sslOptions;
    }
}
