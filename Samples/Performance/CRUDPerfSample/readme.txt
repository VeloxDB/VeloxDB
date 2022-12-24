How to run the sample (with a single node):
1. Build all three sample projects (API, Client and Server), preferably in Release configuration.
	This should yield you API.dll, Client.dll and Server.dll files (in their respective bin folders).
3. Copy API.dll and Server.dll to a new empty directory
4. Unpack the VeloxDB server (the free version is sufficient)
5. Open the file vlxdbcfg.json (in the unpacked server directory)
6. Modify the value of the property BufferPoolSize to 134217728 (128 MB).
	This increases connection buffer pool size which is neccessary for acheiving extremly high transaction throughput.
7. Modify the value of the properties SystemDatabasePath and Log->Path
	Make sure that both directories exist
8. Open the file config.cluster.json (in the unpacked server directory) and replace the localhost with the machine host name or ip address (do not use 0.0.0.0)
	This step is neccessary only if test client is not run locally.
9. Start the database server by running the CLI command:
	vlxdbsrv --interactive
10. Create a single log file for the database by running the following CLI command (while still in unpacked server directory):
	vlx create-log --name Log0 --dir log_path --snapshot-dir snapshot_path --size 10000
		log_path and snapshot_path are path to directories where database log and snapshot files will be stored.
	For best performance it is recommended to store log files and snapshot files on separate storage devices.
11. Deploy the server side files to the server by running the following CLI command:
	vlx update-assemblies --dir assemblies_directory_path --no-confirm
		assemblies_directory_path is the path where you copied API.dll and Server.dll
12. Run the client test application (from the bin directory of the Client project) by running the following CLI command:
	client host_name multiplier worker_count conn_count
		host_name is the host name (or address) that u used in step 8, (uou can use localhost).
			For best performance keep the client and server on separate machines.
			Also, for acheiving the peak peformance network bandwith of the server needs to be more than 1Gbps.
		multiplier determines the size of the dataset (number of Vehicle and Ride objects inserted into the database).
			Spcify 1 to insert 8 million vehicles and 8 million rides, 2 for 16 and so on...
		worker_count number of concurrent requests
		conn_count specifies the number of connections that will be be opened to the database. Good value for this are between 2 and 4.


To run the sample with an HA cluster:
1. Run steps 1 to 7 (in step 4, you will need the enterprise version of VeloxDB database because of cluster support being needed)
2. Provision two separate machines, one for Primary node and one for Standby node
3. Provide a shared network location (SMB) accessible to both machines (with read/write access rights)
4. Create a cluster configuration file in the server directory by running the following CLI commands (for this we use the interactive mode of the client tool)
	vlx
	cluster-config
	new
	create-ha --name main --host1 host_name1 --host2 host_name2 --witness shared_witness_path
		Instead of host_name1 and host_name2 specify the host names of the machines, shared_witness_path is the path to a shared network folder
			
	save --file config.cluster.json
	show
		/*You should see the configuration of the cluster similar to the following:
		main (HA)                 Election timeout: 2.00 s
		├─Shared Folder Witness   Path: witness_path, File timeout: 2 s.
		├─node1 (Write)
		└─node2 (Write)
		*/
	exit
	exit
	
5. Copy the an entire server directory that we just created the cluster configuration in, to the second of the two provisioned machines
6. Open vlxdbcfg.json on both machines and modify the property ThisNodeName to host_name1 and host_name2 respectively
7. Run the VeloxDB server on both machines (step 9 in the previous section)
8. Run the following CLI command on one of the machines (from inside the server directory) to confirm that the cluster is up and running and that the Primary node has been choosen:
	vlx status 
9. Run steps 10 to 12 from the previous section (on any of the two server machines)
10. Run the client application (step 12 of the precious section), but instead of specifying a single host name, specify host names of both server machines:
	client host_name1/host_name2 multiplier worker_count conn_count
