How to run the sample (with a single node):
1. Build all three sample projects (API, Client and Server), preferably in Release configuration.
	This should yield you API.dll, Client.dll and Server.dll files (in their respective bin folders).
3. Copy API.dll and Server.dll to a new empty directory
4. Unpack the VeloxDB server (the free version is sufficient)
5. Open the file vlxdbcfg.json (in the unpacked server directory)
6. Modify the value of the property BufferPoolSize to 134217728 (128 MB).
	This increases connection buffer pool size which is neccessary for acheiving extremly high transaction throughput.
7. Modify the value of the properties SystemDatabasePath and Log->Path
	Make sure that this both directories exist
8. Oepn the file config.cluster.json (in the unpacked server directory) and replace the localhost with the machine host name or ip address (do not use 0.0.0.0)
	This step is neccessary only if test client is not run locally.
9. Start the database server by running the CLI command:
	dotnet vlxdbsrv.dll --interactive
10. Create a single log file for the database by running the following CLI command (while still in unpacked server directory):
	dotnet vlx.dll create-log --name Log0 --dir log_path --snapshot-dir snapshot_path --size 3072
		log_path and snapshot_path are path to directories where database log and snapshot files will be stored.
	For best performance it is recommended to store log files and snapshot files on separate drives.
12. Deploy the server side files to the server by running the following CLI command:
	dotnet vlx.dll update-assemblies --dir assemblies_directory_path --no-confirm
		assemblies_directory_path is the path where you copied API.dll and Server.dll
12. Run the client test application (from the bin directory of the Client project) by running the following CLI command:
	dotnet client.dll host_name multiplier worker_count conn_count
		host_name is the host name of the machine where database server is deplyoed (You can use localhost or ip address here).
			For best performance keep the client and server on separate machines.
		multiplier determines the size of the dataset (number of Vehicle objects inserted into the database).
			Spcify 1 to insert 8 million vehicles, 2 for 16 million vehicles and so on...
		worker_count specifies the number of worker threads applying the changes to the database. This number simulates the number of concurrent users in a real world scenario.
		conn_count specifies the number of connections that will be be opened to the database. This number should be between 1 and 4.


To run the sample with a cluster of two nodes:
1. Run steps 1 to 8 (in step 4, you will need the standard version of VeloxDB database for cluster support)
2. Provision two separate machines, one for Primary node and one for Standby node
3. Provide a shared network location (SMB) accessible to both machines (with write access rights)
4. Create cluster configuration file by running the following CLI commands (for this we use the interactive mode of the client tool)
	dotnet vlx.dll
	cluster-config
	new
	create-ha --name main --host1 host_name1 --host2 host_name2 --witness shared_witness_path
		Instead of host_name1 and host_name2 specify the host names of the machines, shared_witness_path is the path to a shared network folder
			
	save --file config.cluster.json
	show
		You should see the configuration of the cluster similar to the following:
		main (HA)                 Election timeout: 2.00 s
		├─Shared Folder Witness   Path: witness, File timeout: 2 s.
		├─node1 (Write)
		└─node2 (Write)
		
	exit
	exit
	
5. Copy the unpacked server directory (with modified configuration and cluster configuration inside it) to the two provisioned machines
6. Open vlxdbcfg.json on both machines and modify the property ThisNodeName to host_name1 and host_name2 respectively
6. Run the VeloxDB server on both machines (step 9 in the previous section)
7. Run the following CLI command on one of the machines (from inside the server directory) to confirm that the cluster is up and running and that the Primary node has been choosen:
	dotnet vlx.dll status 
	exit
8. Run steps 9 to 10 from the previous section (on any of the two server machines)
9. Run the client application (step 12 of the precious section), but instead specifying a single host name, specify host names of both server machines:
	dotnet client host_name1/host_name2 multiplier worker_count
