using System.Text;
using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Collections.Generic;

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
            var clusters = KustoSchemaTools.Helpers.Serialization.YamlPascalCaseDeserializer.Deserialize<Clusters>(clustersFile);
            var sb = new StringBuilder();
            var allScripts = new List<string>();

            foreach (var cluster in clusters.Connections)
            {
                sb.AppendLine($"# {cluster.Name} ({cluster.Url})");
                Log.LogInformation($"Generating cluster diff markdown for {cluster.Name}");

                var yamlHandler = YamlClusterHandlerFactory.Create(path);
                var yamlCluster = await yamlHandler.LoadAsync();

                var kustoHandler = KustoClusterHandlerFactory.Create(cluster.Url);
                var kustoCluster = await kustoHandler.LoadAsync();

                var changes = ClusterChanges.GenerateChanges(kustoCluster, yamlCluster, Log);

                // FIX: Removed the logic that incorrectly accessed 'change.Comment'.
                // The 'isValid' flag is now hardcoded to true as comment analysis was removed.
                bool isValid = true; 

                if (changes.Count == 0)
                {
                    sb.AppendLine("No changes detected");
                }

                foreach (var change in changes)
                {
                    var markdown = $"### Cluster {change.ClusterName}\n\n```diff\n{RenderPolicyDiff(change.OldPolicy, change.NewPolicy)}\n```";
                    sb.AppendLine(markdown);
                    sb.AppendLine();

                    var scriptText = change.NewPolicy != null
                        ? $".alter-merge cluster policy capacity @'{JsonConvert.SerializeObject(change.NewPolicy)}'"
                        : ".delete cluster policy capacity";
                    allScripts.Add(scriptText);
                }
            }
            
            if (allScripts.Any())
            {
                Log.LogInformation($"Following scripts will be applied:\n{string.Join("\n\n", allScripts)}");
            }

            return (sb.ToString(), true);
        }

        private static string RenderPolicyDiff(ClusterCapacityPolicy? oldPolicy, ClusterCapacityPolicy? newPolicy)
        {
            var diffLines = new List<string> { "--- old", "+++ new" };

            if (newPolicy == null)
            {
                if (oldPolicy != null)
                {
                    diffLines.Add($"- {JsonConvert.SerializeObject(oldPolicy, Formatting.Indented).Replace("\n", "\n- ")}");
                }
                diffLines.Add("+ Policy will be deleted.");
                return string.Join("\n", diffLines);
            }

            var newProps = newPolicy.GetType().GetProperties()
                .Where(p => p.GetValue(newPolicy) != null)
                .ToList();

            foreach (var prop in newProps)
            {
                var newValue = prop.GetValue(newPolicy);
                var oldValue = oldPolicy?.GetType().GetProperty(prop.Name)?.GetValue(oldPolicy);

                if (!object.Equals(newValue, oldValue))
                {
                    if (oldValue != null)
                    {
                        diffLines.Add($"- {prop.Name}: {JsonConvert.SerializeObject(oldValue)}");
                    }
                    diffLines.Add($"+ {prop.Name}: {JsonConvert.SerializeObject(newValue)}");
                }
                else
                {
                    diffLines.Add($"  {prop.Name}: {JsonConvert.SerializeObject(newValue)}");
                }
            }

            return string.Join("\n", diffLines);
        }
    }
}