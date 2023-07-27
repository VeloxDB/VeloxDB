FROM amd64/buildpack-deps:bullseye-curl AS installer
RUN apt-get update && apt-get install unzip \
	&& wget https://github.com/VeloxDB/VeloxDB/releases/download/0.2.2/vlxdb.0.2.2.zip && unzip vlxdb.0.2.2.zip && rm vlxdb.0.2.2.zip
FROM mcr.microsoft.com/dotnet/runtime:7.0
COPY --from=installer ["/vlxdb", "/opt/vlxdb"]
COPY ./Docker/config.cluster.json /opt/vlxdb/config.cluster.json
RUN find /opt/vlxdb -maxdepth 1 -type f -executable ! -name '*.exe' ! -name '*.dll' -exec ln -s {} /usr/bin \;
ENTRYPOINT ["vlxdbsrv", "--interactive"]