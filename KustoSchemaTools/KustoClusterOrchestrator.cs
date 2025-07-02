using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using Microsoft.Extensions.Logging;

namespace KustoSchemaTools
{
    public class KustoClusterOrchestrator
    {
        public KustoClusterOrchestrator(ILogger<KustoClusterOrchestrator> logger, IKustoClusterHandlerFactory kustoClusterHandlerFactory, IYamlClusterHandlerFactory yamlClusterHandlerFactory)
        {
            Log = logger;
            KustoClusterHandlerFactory = kustoClusterHandlerFactory;
            YamlClusterHandlerFactory = yamlClusterHandlerFactory;
        }

        public ILogger Log { get; }
        public IKustoClusterHandlerFactory KustoClusterHandlerFactory { get; }
        public IYamlClusterHandlerFactory YamlClusterHandlerFactory { get; }

        /// <summary>
        /// Generates changes by comparing the provided cluster configurations with their 
        /// corresponding live Kusto clusters.
        /// </summary>
        /// <param name="clusters">The cluster configurations to compare against live clusters.</param>
        /// <returns>A list of ClusterChangeSet objects representing the detected changes.</returns>
        public async Task<List<ClusterChangeSet>> GenerateChangesAsync(Clusters clusters)
        {
            var allChanges = new List<ClusterChangeSet>();

            foreach (var cluster in clusters.Connections)
            {
                Log.LogInformation($"Generating cluster diff for {cluster.Name}");

                // Load the "old" schema from the live Kusto cluster
                var kustoHandler = KustoClusterHandlerFactory.Create(cluster.Name, cluster.Url);
                var kustoCluster = await kustoHandler.LoadAsync();

                // Compare the live state with the proposed new configuration and generate a change object
                var change = ClusterChanges.GenerateChanges(kustoCluster, cluster, Log);
                allChanges.Add(change);
            }

            return allChanges;
        }

        /// <summary>
        /// Loads cluster configurations from a YAML file and generates changes by comparing 
        /// them with the live Kusto clusters.
        /// </summary>
        /// <param name="clusterConfigFilePath">The path to the YAML file containing cluster configurations.</param>
        /// <returns>A list of ClusterChangeSet objects representing the detected changes.</returns>
        public async Task<List<ClusterChangeSet>> GenerateChangesFromFileAsync(string clusterConfigFilePath)
        {
            Log.LogInformation($"Loading cluster configurations from file: {clusterConfigFilePath}");

            var yamlHandler = YamlClusterHandlerFactory.Create(clusterConfigFilePath);
            var clusterList = await yamlHandler.LoadAsync();
            var clusters = new Clusters
            {
                Connections = clusterList
            };

            Log.LogInformation($"Loaded {clusterList.Count} cluster configuration(s) from YAML file");

            return await GenerateChangesAsync(clusters);
        }
    }
}