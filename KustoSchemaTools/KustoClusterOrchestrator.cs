using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Helpers;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace KustoSchemaTools
{
    public class KustoClusterOrchestrator
    {
        public KustoClusterOrchestrator(ILogger<KustoClusterOrchestrator> logger, YamlClusterHandlerFactory yamlClusterHandlerFactory, KustoClusterHandlerFactory kustoClusterHandlerFactory)
        {
            Log = logger;
            YamlClusterHandlerFactory = yamlClusterHandlerFactory;
            KustoClusterHandlerFactory = kustoClusterHandlerFactory;
        }

        public ILogger Log { get; }
        public YamlClusterHandlerFactory YamlClusterHandlerFactory { get; }
        public KustoClusterHandlerFactory KustoClusterHandlerFactory { get; }

        /// <summary>
        /// Orchestrates loading the cluster definitions from YAML and the live cluster,
        /// and returns a list of objects representing the detected changes.
        /// </summary>
        /// <param name="path">The path to the directory containing the cluster definition files.</param>
        /// <returns>A list of ClusterChange objects.</returns>
        public async Task<List<ClusterChange>> GenerateChangesAsync(string path)
        {
            var clustersFile = File.ReadAllText(Path.Combine(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);
            var allChanges = new List<ClusterChange>();

            foreach (var clusterConnection in clusters.Connections)
            {
                Log.LogInformation($"Generating cluster diff for {clusterConnection.Name}");

                // 1. Load the "new" schema from the local YAML files
                var yamlHandler = YamlClusterHandlerFactory.Create(path);
                var yamlCluster = await yamlHandler.LoadAsync();

                // 2. Load the "old" schema from the live Kusto cluster
                var kustoHandler = KustoClusterHandlerFactory.Create(clusterConnection.Url);
                var kustoCluster = await kustoHandler.LoadAsync();

                // 3. Compare the two and generate a change object
                var change = ClusterChanges.GenerateChanges(kustoCluster, yamlCluster, Log);
                allChanges.Add(change);
            }

            return allChanges;
        }
    }
}