using System.Text;
using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Linq;
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

        public async Task<(string markDown, bool isValid)> GenerateDiffMarkdown(string path)
        {
            var clustersFile = File.ReadAllText(Path.Combine(path, "clusters.yml"));
            var clusters = Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);
            var sb = new StringBuilder();
            bool isValid = true;

            foreach (var cluster in clusters.Connections)
            {
                Log.LogInformation($"Generating cluster-scoped diff markdown for {cluster.Name}");

                var yamlHandler = YamlClusterHandlerFactory.Create(path);
                var yamlCluster = await yamlHandler.LoadAsync();

                var kustoHandler = KustoClusterHandlerFactory.Create(cluster.Url);
                var kustoCluster = await kustoHandler.LoadAsync();

                var changes = ClusterChanges.GenerateChanges(kustoCluster, yamlCluster, Log);

                var comments = changes.Select(itm => itm.Comment).Where(itm => itm != null).ToList();
                isValid &= comments.All(itm => itm.FailsRollout == false);

                sb.AppendLine($"# {cluster.Name} ({cluster.Url})");

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
            }

            return (sb.ToString(), isValid);
        }
    }
}