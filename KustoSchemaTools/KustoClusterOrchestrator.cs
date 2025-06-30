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
                var kustoHandler = KustoClusterHandlerFactory.Create(cluster.Url);
                var kustoCluster = await kustoHandler.LoadAsync();

                // Compare the live state with the proposed new configuration and generate a change object
                var change = ClusterChanges.GenerateChanges(kustoCluster, cluster, Log);
                allChanges.Add(change);
            }

            return allChanges;
        }
    }
}