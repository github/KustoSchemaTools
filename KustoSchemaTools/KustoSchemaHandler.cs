using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Data;
using System.Text;

namespace KustoSchemaTools
{
    public class KustoSchemaHandler<T> where T : Database, new()
    {
        public KustoSchemaHandler(ILogger<KustoSchemaHandler<T>> schemaHandlerLogger, YamlDatabaseHandlerFactory<T> yamlDatabaseHandlerFactory, KustoDatabaseHandlerFactory<T> kustoDatabaseHandlerFactory)
        {
            Log = schemaHandlerLogger;
            YamlDatabaseHandlerFactory = yamlDatabaseHandlerFactory;
            KustoDatabaseHandlerFactory = kustoDatabaseHandlerFactory;
        }

        public ILogger Log { get; }
        public YamlDatabaseHandlerFactory<T> YamlDatabaseHandlerFactory { get; }
        public KustoDatabaseHandlerFactory<T> KustoDatabaseHandlerFactory { get; }

        public async Task<(string markDown, bool isValid)> GenerateDiffMarkdown(string path, string databaseName)
        {

            var clustersFile = File.ReadAllText(Path.Combine(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);
            var sb = new StringBuilder();
            bool isValid = true;

            var yamlHandler = YamlDatabaseHandlerFactory.Create(path, databaseName);
            var yamlDb = await yamlHandler.LoadAsync();

            foreach (var cluster in clusters.Connections)
            {
                Log.LogInformation($"Generating diff markdown for {Path.Combine(path, databaseName)} => {cluster}/{databaseName}");

                var dbHandler = KustoDatabaseHandlerFactory.Create(cluster.Url, databaseName);
                var kustoDb = await dbHandler.LoadAsync();
                var changes = DatabaseChanges.GenerateChanges(kustoDb, yamlDb, databaseName, Log);

                isValid &= changes.All(itm => itm.Scripts.All(itm => itm.IsValid != false));

                sb.AppendLine($"# {cluster.Name}/{databaseName} ({cluster.Url})");

                if(changes.Count == 0)
                {
                    sb.AppendLine("No changes detected");
                }

                foreach (var change in changes)
                {
                    sb.AppendLine(change.Markdown);
                    sb.AppendLine();
                    sb.AppendLine();
                }

                var scriptSb = new StringBuilder();
                foreach(var script in changes.SelectMany(itm => itm.Scripts).Where(itm => itm.IsValid == true).OrderBy(itm => itm.Order))
                {
                    scriptSb.AppendLine(script.Text);
                }

                Log.LogInformation($"Following scripts will be applied:\n{scriptSb}");
            }
            return (sb.ToString(), isValid);
        }

        public async Task Import(string path, string databaseName, bool includeColumns)
        {
            var clustersFile = File.ReadAllText(Path.Combine(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);

            var dbHandler = KustoDatabaseHandlerFactory.Create(clusters.Connections[0].Url, databaseName);

            var db = await dbHandler.LoadAsync();
            if (includeColumns == false)
            {
                foreach(var table in db.Tables.Values)
                {
                    table.Columns = new Dictionary<string, string>();
                }
            }

            var fileHandler = YamlDatabaseHandlerFactory.Create(path, databaseName);
            await fileHandler.WriteAsync(db);
        }


        public async Task<ConcurrentDictionary<string,Exception>> Apply(string path, string databaseName)
        {
            var clustersFile = File.ReadAllText(Path.Combine(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);

            var yamlHandler = YamlDatabaseHandlerFactory.Create(path, databaseName);
            var yamlDb = await yamlHandler.LoadAsync();

            var results = new ConcurrentDictionary<string,Exception>();

            await Parallel.ForEachAsync(clusters.Connections, new ParallelOptions { MaxDegreeOfParallelism = 1}, async (cluster, token) =>
            {
                try
                {
                    Log.LogInformation($"Generating and applying script for {Path.Combine(path, databaseName)} => {cluster}/{databaseName}");
                    var dbHandler = KustoDatabaseHandlerFactory.Create(cluster.Url, databaseName);
                    await dbHandler.WriteAsync(yamlDb);
                    results.TryAdd(cluster.Url, null);
                }
                catch (Exception ex)
                {
                    results.TryAdd(cluster.Url, ex);
                }
            });

            return results;
        }
    }
}
