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

                return clusters.Connections.ToList();
            }
            catch (Exception ex) when (!(ex is FileNotFoundException || ex is InvalidOperationException))
            {
                throw new InvalidOperationException($"Failed to parse clusters file '{_filePath}': {ex.Message}", ex);
            }
        }
    }
}