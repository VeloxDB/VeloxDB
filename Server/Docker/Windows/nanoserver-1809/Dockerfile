FROM mcr.microsoft.com/powershell:lts-nanoserver-1809 as installer
ARG version
SHELL ["pwsh", "-command"]
RUN write-host $env:version
WORKDIR C:/Users/ContainerUser
RUN Invoke-WebRequest "https://github.com/VeloxDB/VeloxDB/releases/download/$env:version/vlxdb.$env:version.zip" -OutFile vlxdb.zip; Expand-Archive vlxdb.zip; Remove-Item vlxdb.zip
FROM mcr.microsoft.com/dotnet/runtime:8.0-nanoserver-1809
ARG version
COPY --from=installer ["C:/Users/ContainerUser/vlxdb/vlxdb", "C:/Program Files/vlxdb"]
COPY ["./config.cluster.json", "C:/Program Files/vlxdb/config.cluster.json"]
EXPOSE 7568
ENTRYPOINT ["C:/Program Files/vlxdb/vlxdbsrv.exe", "--interactive"]
