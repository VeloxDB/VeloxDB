FROM mcr.microsoft.com/dotnet/sdk:7.0
WORKDIR /source
COPY . .
RUN dotnet build -c:Release
ENTRYPOINT [ "/usr/bin/env", "dotnet", "run", "-c:Release" ]