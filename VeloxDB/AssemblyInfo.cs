using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("vlxdbsrv")]
[assembly: InternalsVisibleTo("vlxdbem")]
[assembly: InternalsVisibleTo("vlxrep")]

// Internally generated assembly
[assembly: InternalsVisibleTo("__ObjectModel")]
[assembly: InternalsVisibleTo("__IndexReaders")]

#if TEST_BUILD
[assembly: InternalsVisibleTo("CommonTestSuite")]
[assembly: InternalsVisibleTo("ProtocolTestSuite")]
[assembly: InternalsVisibleTo("NetworkingTestSuite")]
[assembly: InternalsVisibleTo("MetaTestSuite")]
[assembly: InternalsVisibleTo("StorageEngineTestSuite")]
[assembly: InternalsVisibleTo("stressrun")]
[assembly: InternalsVisibleTo("ServerTestSuite")]
#endif