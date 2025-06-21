using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Parser.KustoLoader;
using KustoSchemaTools.Parser.KustoWriter;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
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

            var escapedDbName = databaseName.BracketIfIdentifier();


            foreach (var cluster in clusters.Connections)
            {
                Log.LogInformation($"Generating diff markdown for {Path.Combine(path, databaseName)} => {cluster}/{escapedDbName}");

                var dbHandler = CreateDatabaseHandlerWithCapacityPolicy(cluster.Url, escapedDbName, clusters.CapacityPolicy);
                var kustoDb = await dbHandler.LoadAsync();
                var changes = DatabaseChanges.GenerateChanges(kustoDb, yamlDb, escapedDbName, Log);

                var comments = changes.Select(itm => itm.Comment).Where(itm => itm != null).ToList();


                isValid &= changes.All(itm => itm.Scripts.All(itm => itm.IsValid != false)) && comments.All(itm => itm.FailsRollout == false);

                sb.AppendLine($"# {cluster.Name}/{escapedDbName} ({cluster.Url})");

                foreach (var comment in comments)
                {
                    sb.AppendLine($"> [!{comment.Kind.ToString().ToUpper()}]");
                    sb.AppendLine($"> {comment.Text}");
                    sb.AppendLine();
                }

                if (changes.Count == 0)
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
                foreach (var script in changes.SelectMany(itm => itm.Scripts).Where(itm => itm.IsValid == true).OrderBy(itm => itm.Order))
                {
                    scriptSb.AppendLine(script.Text);
                }

                Log.LogInformation($"Following scripts will be applied:\n{scriptSb}");
            }

            foreach (var follower in yamlDb.Followers)
            {

                Log.LogInformation($"Generating diff markdown for {Path.Combine(path, databaseName)} => {follower.Key}/{follower.Value.DatabaseName}");


                var followerClient = new KustoClient(follower.Key);
                var oldModel = FollowerLoader.LoadFollower(follower.Value.DatabaseName, followerClient);

                var newModel = follower.Value;

                var changes = DatabaseChanges.GenerateFollowerChanges(oldModel, newModel, Log);

                sb.AppendLine($"# Changes for follower database {follower.Key}/{follower.Value.DatabaseName}");
                sb.AppendLine();
                foreach (var change in changes)
                {
                    sb.AppendLine(change.Markdown);
                    sb.AppendLine();
                }
            }
            return (sb.ToString(), isValid);
        }

        public async Task Import(string path, string databaseName, bool includeColumns)
        {
            var clustersFile = File.ReadAllText(Path.Combine(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);

            var escapedDbName = databaseName.BracketIfIdentifier();
            var dbHandler = CreateDatabaseHandlerWithCapacityPolicy(clusters.Connections[0].Url, escapedDbName, clusters.CapacityPolicy);

            var db = await dbHandler.LoadAsync();
            if (includeColumns == false)
            {
                foreach (var table in db.Tables.Values)
                {
                    table.Columns = new Dictionary<string, string>();
                }
            }

            var fileHandler = YamlDatabaseHandlerFactory.Create(path, databaseName);
            await fileHandler.WriteAsync(db);
        }


        public async Task<ConcurrentDictionary<string, Exception>> Apply(string path, string databaseName)
        {
            var clustersFile = File.ReadAllText(Path.Combine(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);

            var escapedDbName = databaseName.BracketIfIdentifier();
            var yamlHandler = YamlDatabaseHandlerFactory.Create(path, databaseName);
            var yamlDb = await yamlHandler.LoadAsync();

            var results = new ConcurrentDictionary<string, Exception>();

            await Parallel.ForEachAsync(clusters.Connections, async (cluster, token) =>
            {
                try
                {
                    Log.LogInformation($"Generating and applying script for {Path.Combine(path, databaseName)} => {cluster}/{escapedDbName}");

                    // Create the database handler with capacity policy writer if needed
                    var dbHandler = CreateDatabaseHandlerWithCapacityPolicy(cluster.Url, escapedDbName, clusters.CapacityPolicy);
                    await dbHandler.WriteAsync(yamlDb);
                    results.TryAdd(cluster.Url, null!);
                }
                catch (Exception ex)
                {
                    results.TryAdd(cluster.Url, ex);
                }
            });

            return results;
        }

        private IDatabaseHandler<T> CreateDatabaseHandlerWithCapacityPolicy(string clusterUrl, string databaseName, ClusterCapacityPolicy? capacityPolicy)
        {
            // Create a logger - in a real application this would come from DI
            var loggerFactory = LoggerFactory.Create(builder => { });
            var logger = loggerFactory.CreateLogger<KustoDatabaseHandler<T>>();

            var factory = new KustoDatabaseHandlerFactory<T>(logger)
                .WithReader<KustoDatabasePrincipalLoader>()
                .WithReader<KustoDatabaseRetentionAndCacheLoader>()
                .WithReader<KustoTableBulkLoader>()
                .WithReader<KustoFunctionBulkLoader>()
                .WithReader<KustoMaterializedViewBulkLoader>()
                .WithReader<KustoExternalTableBulkLoader>()
                .WithReader<KustoContinuousExportBulkLoader>()
                .WithReader<KustoEntityGroupBulkLoader>()
                .WithReader<KustoPartitioningPolicyLoader>()
                .WithReader<ClusterCapacityPolicyLoader>()
                .WithReader<DatabaseCleanup>()
                .WithWriter<DefaultDatabaseWriter>();

            // Add capacity policy writer if policy is defined
            if (capacityPolicy != null)
            {
                factory.WithPlugin(new ClusterCapacityPolicyWriter(capacityPolicy));
            }

            return factory.Create(clusterUrl, databaseName);
        }
    }
}
