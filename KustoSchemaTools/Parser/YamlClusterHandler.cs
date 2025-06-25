using KustoSchemaTools.Model;
using System.IO;
using System.Threading.Tasks;

namespace KustoSchemaTools
{
    public class YamlClusterHandler
    {
        private readonly string _path;

        public YamlClusterHandler(string path)
        {
            _path = path;
        }

        public Task<Cluster> LoadAsync()
        {
            var clustersFile = File.ReadAllText(Path.Combine(_path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);
            
            // TODO -- handle case with multiple clusters
            var cluster = new Cluster
            {
                CapacityPolicy = clusters.Connections.FirstOrDefault()?.CapacityPolicy
            };

            return Task.FromResult(cluster);
        }
    }
}