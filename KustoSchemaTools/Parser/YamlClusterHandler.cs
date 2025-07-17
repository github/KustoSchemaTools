using KustoSchemaTools.Model;
using KustoSchemaTools.Helpers;

namespace KustoSchemaTools
{
    public class YamlClusterHandler
    {
        private readonly string _filePath;

        public YamlClusterHandler(string filePath)
        {
            _filePath = filePath;
        }

        public async Task<List<Cluster>> LoadAsync()
        {
            try
            {
                if (!File.Exists(_filePath))
                {
                    throw new FileNotFoundException($"Clusters file not found at path: {_filePath}");
                }

                var clustersFileContent = await File.ReadAllTextAsync(_filePath);
                
                if (string.IsNullOrWhiteSpace(clustersFileContent))
                {
                    throw new InvalidOperationException($"Clusters file is empty: {_filePath}");
                }

                var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFileContent);

                ValidateClusters(clusters);

                return clusters.Connections.ToList();
            }
            catch (Exception ex) when (!(ex is FileNotFoundException || ex is InvalidOperationException))
            {
                throw new InvalidOperationException($"Failed to parse clusters file '{_filePath}': {ex.Message}", ex);
            }
        }

        private static void ValidateClusters(Clusters clusters)
        {
            if (clusters?.Connections == null)
                return;

            for (int clusterIndex = 0; clusterIndex < clusters.Connections.Count; clusterIndex++)
            {
                var cluster = clusters.Connections[clusterIndex];
                
                // Validate cluster basic properties
                if (string.IsNullOrWhiteSpace(cluster.Name))
                {
                    throw new InvalidOperationException($"Cluster at index {clusterIndex} is missing a required 'name' property.");
                }
                
                if (string.IsNullOrWhiteSpace(cluster.Url))
                {
                    throw new InvalidOperationException($"Cluster '{cluster.Name}' is missing a required 'url' property.");
                }
                
                // Validate workload groups
                if (cluster.WorkloadGroups?.Count > 0)
                {
                    for (int wgIndex = 0; wgIndex < cluster.WorkloadGroups.Count; wgIndex++)
                    {
                        var workloadGroup = cluster.WorkloadGroups[wgIndex];
                        
                        if (string.IsNullOrWhiteSpace(workloadGroup.WorkloadGroupName))
                        {
                            throw new InvalidOperationException(
                                $"Cluster '{cluster.Name}' has a workload group at index {wgIndex} that is missing a required 'workloadGroupName' property. " +
                                "All workload groups must have a non-empty name.");
                        }
                    }
                }
            }
        }
    }
}