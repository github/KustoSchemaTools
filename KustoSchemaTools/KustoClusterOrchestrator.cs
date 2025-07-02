using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using Microsoft.Extensions.Logging;
using Kusto.Data;

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

        /// <summary>
        /// Loads cluster configurations from a YAML file, generates changes by comparing 
        /// them with the live Kusto clusters, and then applies those changes.
        /// </summary>
        /// <param name="clusterConfigFilePath">The path to the YAML file containing cluster configurations.</param>
        /// <returns>A task representing the asynchronous apply operation.</returns>
        public async Task<List<ScriptExecuteCommandResult>> ApplyAsync(string clusterConfigFilePath)
        {
            Log.LogInformation($"Starting apply operation for cluster config file: {clusterConfigFilePath}");

            // Generate the changes first
            var changeSets = await GenerateChangesFromFileAsync(clusterConfigFilePath);
            var allResults = new List<ScriptExecuteCommandResult>();

            if (!changeSets.Any(cs => cs.Changes.SelectMany(itm => itm.Scripts).Where(itm => itm.Order >= 0).Where(itm => itm.IsValid == true).Any()))
            {
                Log.LogInformation("No changes detected to apply.");
                return allResults;
            }

            // Apply changes for each cluster
            foreach (var changeSet in changeSets.Where(cs => cs.Changes.SelectMany(itm => itm.Scripts).Where(itm => itm.Order >= 0).Where(itm => itm.IsValid == true).Any()))
            {
                Log.LogInformation($"Applying changes to cluster: {changeSet.Entity}");

                var clusterName = changeSet.To.Name;
                var clusterUrl = changeSet.To.Url;

                var kustoHandler = KustoClusterHandlerFactory.Create(clusterName, clusterUrl);
                var result = await kustoHandler.WriteAsync(changeSet);
                
                // Add the results from this cluster to the overall results list
                allResults.AddRange(result);
            }

            Log.LogInformation($"Finished applying. Total scripts executed: {allResults.Count}");
            return allResults;
        }
    }
}