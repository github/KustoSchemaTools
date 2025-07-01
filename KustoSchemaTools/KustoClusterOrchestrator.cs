using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Helpers;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KustoSchemaTools
{
    public class KustoClusterOrchestrator
    {
        public KustoClusterOrchestrator(ILogger<KustoClusterOrchestrator> logger, IKustoClusterHandlerFactory kustoClusterHandlerFactory)
        {
            Log = logger;
            KustoClusterHandlerFactory = kustoClusterHandlerFactory;
        }

        public ILogger Log { get; }
        public IKustoClusterHandlerFactory KustoClusterHandlerFactory { get; }

        /// <summary>
        /// Orchestrates loading the cluster definitions from YAML and the live cluster,
        /// and returns a list of objects representing the detected changes.
        /// </summary>
        /// <param name="path">The path to the directory containing the cluster definition files.</param>
        /// <param name="clusters">The cluster definitions loaded from configuration.</param>
        /// <returns>A list of ClusterChange objects.</returns>
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

            // Load clusters from YAML file
            var yamlHandler = new YamlClusterHandler(clusterConfigFilePath);
            var clusterList = await yamlHandler.LoadAsync();

            // Create Clusters object from the loaded list
            var clusters = new Clusters
            {
                Connections = clusterList
            };

            Log.LogInformation($"Loaded {clusterList.Count} cluster configuration(s) from YAML file");

            // Generate changes using the existing method
            return await GenerateChangesAsync(clusters);
        }
    }
}