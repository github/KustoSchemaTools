using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Parser.KustoLoader;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Channels;

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
            var diffData = await BuildDiffComputationResult(path, databaseName);
            return BuildMarkdownOutput(diffData, path, databaseName, logDetails: true);
        }

        public async Task<StructuredDiffResult> GenerateStructuredDiff(string path, string databaseName)
        {
            var diffData = await BuildDiffComputationResult(path, databaseName);

            var structuredDiffs = new List<StructuredDiff>();

            foreach (var clusterDiff in diffData.ClusterDiffs)
            {
                structuredDiffs.Add(ConvertToStructuredDiff(clusterDiff.Cluster.Name, clusterDiff.Cluster.Url, databaseName, clusterDiff.Changes));
            }

            foreach (var followerDiff in diffData.FollowerDiffs)
            {
                structuredDiffs.Add(ConvertToStructuredDiff(followerDiff.ConnectionKey, followerDiff.ConnectionKey, followerDiff.DatabaseName, followerDiff.Changes));
            }

            var result = new StructuredDiffResult
            {
                Diffs = structuredDiffs,
                IsValid = structuredDiffs.All(diff => diff.IsValid)
            };

            var markdownPreview = BuildMarkdownOutput(diffData, path, databaseName, logDetails: false);
            if (markdownPreview.markDown.Length > 50000)
            {
                result.Message = "The generated output is too long to be posted. Please get the results of the planning from the logs in the actions run.";
            }

            return result;
        }

        public async Task Import(string path, string databaseName, bool includeColumns)
        {
            var clustersFile = File.ReadAllText(Path.Join(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);

            var dbHandler = KustoDatabaseHandlerFactory.Create(clusters.Connections[0].Url, databaseName);

            var db = await dbHandler.LoadAsync();
            if (!includeColumns)
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
            var clustersFile = File.ReadAllText(Path.Join(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);

            var yamlHandler = YamlDatabaseHandlerFactory.Create(path, databaseName);
            var yamlDb = await yamlHandler.LoadAsync();

            var results = new ConcurrentDictionary<string,Exception>();

            await Parallel.ForEachAsync(clusters.Connections, async (cluster, token) =>
            {
                try
                {
                    Log.LogInformation($"Generating and applying script for {Path.Join(path, databaseName)} => {cluster}/{databaseName}");
                    var dbHandler = KustoDatabaseHandlerFactory.Create(cluster.Url, databaseName);
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

        private async Task<DiffComputationResult> BuildDiffComputationResult(string path, string databaseName)
        {
            var clustersFile = File.ReadAllText(Path.Join(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);

            var yamlHandler = YamlDatabaseHandlerFactory.Create(path, databaseName);
            var yamlDb = await yamlHandler.LoadAsync();

            var clusterDiffs = new List<ClusterDiffContext>();
            foreach (var cluster in clusters.Connections)
            {
                var dbHandler = KustoDatabaseHandlerFactory.Create(cluster.Url, databaseName);
                var kustoDb = await dbHandler.LoadAsync();
                var changes = DatabaseChanges.GenerateChanges(kustoDb, yamlDb, databaseName, Log);
                clusterDiffs.Add(new ClusterDiffContext(cluster, changes));
            }

            var followerDiffs = new List<FollowerDiffContext>();
            foreach (var follower in yamlDb.Followers)
            {
                var followerClient = new KustoClient(follower.Key);
                var oldModel = FollowerLoader.LoadFollower(follower.Value.DatabaseName, followerClient);
                var changes = DatabaseChanges.GenerateFollowerChanges(oldModel, follower.Value, Log);
                followerDiffs.Add(new FollowerDiffContext(follower.Key, follower.Value.DatabaseName, changes));
            }

            return new DiffComputationResult(clusterDiffs, followerDiffs);
        }

        private (string markDown, bool isValid) BuildMarkdownOutput(DiffComputationResult diffData, string path, string databaseName, bool logDetails)
        {
            var sb = new StringBuilder();
            bool isValid = true;

            foreach (var clusterDiff in diffData.ClusterDiffs)
            {
                if (logDetails)
                {
                    Log.LogInformation($"Generating diff markdown for {Path.Join(path, databaseName)} => {clusterDiff.Cluster.Name}/{databaseName}");
                }

                var changes = clusterDiff.Changes;
                var comments = changes.Select(change => change.Comment).OfType<Comment>().ToList();
                var clusterValid = IsDiffValid(changes);
                isValid &= clusterValid;

                sb.AppendLine($"# {clusterDiff.Cluster.Name}/{databaseName} ({clusterDiff.Cluster.Url})");

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

                if (logDetails)
                {
                    var scriptSb = new StringBuilder();
                    foreach (var script in changes.SelectMany(itm => itm.Scripts).Where(itm => itm.IsValid is true).OrderBy(itm => itm.Script.Order))
                    {
                        scriptSb.AppendLine(script.Script.Text);
                    }

                    Log.LogInformation($"Following scripts will be applied:\n{scriptSb}");
                }
            }

            foreach (var followerDiff in diffData.FollowerDiffs)
            {
                if (logDetails)
                {
                    Log.LogInformation($"Generating diff markdown for {Path.Join(path, databaseName)} => {followerDiff.ConnectionKey}/{followerDiff.DatabaseName}");
                }

                sb.AppendLine($"# Changes for follower database {followerDiff.ConnectionKey}/{followerDiff.DatabaseName}");
                sb.AppendLine();
                foreach (var change in followerDiff.Changes)
                {
                    sb.AppendLine(change.Markdown);
                    sb.AppendLine();
                }

                var followerValid = IsDiffValid(followerDiff.Changes);
                isValid &= followerValid;
            }

            return (sb.ToString(), isValid);
        }

        private StructuredDiff ConvertToStructuredDiff(string clusterName, string clusterUrl, string databaseName, List<IChange> changes)
        {
            var structuredChanges = changes.Select(change => change.ToStructuredChange()).ToList();
            var comments = changes
                .Select(change => StructuredComment.From(change.Comment))
                .OfType<StructuredComment>()
                .ToList();

            var validScripts = changes
                .SelectMany(change => change.Scripts)
                .Where(script => script.IsValid is true)
                .OrderBy(script => script.Script.Order)
                .ToList();

            return new StructuredDiff
            {
                ClusterName = clusterName,
                ClusterUrl = clusterUrl,
                DatabaseName = databaseName,
                IsValid = IsDiffValid(changes),
                Comments = comments,
                Changes = structuredChanges,
                ValidScripts = validScripts
            };
        }

        private static bool IsDiffValid(IEnumerable<IChange> changes)
        {
            var changeList = changes?.ToList() ?? new List<IChange>();
            var scriptsHealthy = changeList.All(change => change.Scripts.All(script => script.IsValid is not false));
            var commentsHealthy = changeList
                .Select(change => change.Comment)
                .OfType<Comment>()
                .All(comment => comment.FailsRollout is false);

            return scriptsHealthy && commentsHealthy;
        }

        private sealed class DiffComputationResult
        {
            public DiffComputationResult(List<ClusterDiffContext> clusterDiffs, List<FollowerDiffContext> followerDiffs)
            {
                ClusterDiffs = clusterDiffs;
                FollowerDiffs = followerDiffs;
            }

            public List<ClusterDiffContext> ClusterDiffs { get; }
            public List<FollowerDiffContext> FollowerDiffs { get; }
        }

        private sealed class ClusterDiffContext
        {
            public ClusterDiffContext(Cluster cluster, List<IChange> changes)
            {
                Cluster = cluster;
                Changes = changes;
            }

            public Cluster Cluster { get; }
            public List<IChange> Changes { get; }
        }

        private sealed class FollowerDiffContext
        {
            public FollowerDiffContext(string connectionKey, string databaseName, List<IChange> changes)
            {
                ConnectionKey = connectionKey;
                DatabaseName = databaseName;
                Changes = changes;
            }

            public string ConnectionKey { get; }
            public string DatabaseName { get; }
            public List<IChange> Changes { get; }
        }
    }
}
