FROM amd64/buildpack-deps:bullseye-curl AS installer
ARG version
RUN apt-get update && apt-get install unzip \
	&& wget https://github.com/VeloxDB/VeloxDB/releases/download/$version/vlxdb.$version.zip && unzip vlxdb.$version.zip && rm vlxdb.$version.zip
FROM mcr.microsoft.com/dotnet/runtime:7.0
COPY --from=installer ["/vlxdb", "/opt/vlxdb"]
COPY ./config.cluster.json /opt/vlxdb/config.cluster.json
RUN find /opt/vlxdb -maxdepth 1 -type f -executable ! -name '*.exe' ! -name '*.dll' -exec ln -s {} /usr/bin \;
EXPOSE 7568
ENTRYPOINT ["vlxdbsrv", "--interactive"]