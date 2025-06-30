using KustoSchemaTools.Model;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System;
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
                
                if (clusters?.Connections == null)
                {
                    throw new InvalidOperationException($"Invalid clusters file format. Expected 'connections' property not found in: {_filePath}");
                }

                // Validate that each cluster has required properties
                foreach (var cluster in clusters.Connections)
                {
                    if (string.IsNullOrWhiteSpace(cluster.Name))
                    {
                        throw new InvalidOperationException($"Cluster missing required 'name' property in file: {_filePath}");
                    }
                    
                    if (string.IsNullOrWhiteSpace(cluster.Url))
                    {
                        throw new InvalidOperationException($"Cluster '{cluster.Name}' missing required 'url' property in file: {_filePath}");
                    }
                }

                return clusters.Connections.ToList();
            }
            catch (Exception ex) when (!(ex is FileNotFoundException || ex is InvalidOperationException))
            {
                throw new InvalidOperationException($"Failed to parse clusters file '{_filePath}': {ex.Message}", ex);
            }
        }
    }
}