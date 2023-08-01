#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
RUN apt-get update && apt-get install -y git curl
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build

COPY ["KustoSchemaTools.Cli/KustoSchemaTools.Cli.csproj", "KustoSchemaTools.Cli/"]
COPY ["KustoSchemaTools/KustoSchemaTools.csproj", "KustoSchemaTools/"]
RUN dotnet restore "KustoSchemaTools.Cli/KustoSchemaTools.Cli.csproj"
COPY . .
WORKDIR "/KustoSchemaTools.Cli"
RUN dotnet build "KustoSchemaTools.Cli.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "KustoSchemaTools.Cli.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "KustoSchemaTools.Cli.dll"]