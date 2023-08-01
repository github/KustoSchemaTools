using CommandLine;
using KustoSchemaTools;
using KustoSchemaTools.Cli;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Parser.KustoLoader;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using static CommandLine.Parser;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton(new YamlDatabaseHandlerFactory()
    .WithPlugin(new TablePlugin())
    .WithPlugin(new FunctionPlugin())
    .WithPlugin(new MaterializedViewsPlugin())
    .WithPlugin(new DatabaseCleanup()));

builder.Services.AddSingleton( sp =>  new KustoDatabaseHandlerFactory(sp.GetService<ILogger<KustoDatabaseHandler>>())
    .WithPlugin<KustoDatabasePrincipalLoader>()
    .WithPlugin<KustoDatabaseRetentionAndCacheLoader>()
    .WithPlugin<KustoTableBulkLoader>()
    .WithPlugin<KustoFunctionBulkLoader>()
    .WithPlugin<KustoMaterializedViewBulkLoader>()
    .WithPlugin<DatabaseCleanup>()
    );
builder.Services.AddSingleton<KustoSchemaHandler>();
using IHost host = builder.Build();

ParserResult<ActionInputs> parser = Default.ParseArguments<ActionInputs>(() => new(), args);
var logger = host.Services
            .GetRequiredService<ILoggerFactory>()
            .CreateLogger("KustoSchemaTools.Cli.Program");

parser.WithNotParsed(
    errors =>
    {   logger.LogError("{Errors}", string.Join(Environment.NewLine, errors.Select(error => error.ToString())));
        Environment.Exit(2);
    });

await parser.WithParsedAsync(async itm =>
{

    var handler = host.Services.GetService<KustoSchemaHandler>();
    await DefaultActionInputHandler.StartAnalysisAsync(itm, handler, logger);
});

await host.RunAsync();
