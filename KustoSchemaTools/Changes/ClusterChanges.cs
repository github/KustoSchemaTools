using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Linq;
using Kusto.Language;

namespace KustoSchemaTools.Changes
{
    public class ClusterChanges
    {
        public static ClusterChangeSet GenerateChanges(Cluster oldCluster, Cluster newCluster, ILogger log)
        {
            if (oldCluster.Name != newCluster.Name)
            {
                throw new ArgumentException($"Cluster names must match; {oldCluster.Name} != {newCluster.Name}");
            }
            var clusterName = oldCluster.Name;
            var changeSet = new ClusterChangeSet(clusterName, oldCluster, newCluster);

            log.LogInformation($"Analyzing capacity policy changes for cluster {clusterName}...");
            if (newCluster.CapacityPolicy == null) {
                log.LogInformation("No capacity policy defined in the new cluster configuration.");
            } else {
                var capacityPolicyChange = ComparePolicy(
                    "Cluster Capacity Policy",
                    "default",
                    oldCluster.CapacityPolicy,
                    newCluster.CapacityPolicy,
                    policy => new List<DatabaseScriptContainer> {
                    new DatabaseScriptContainer("AlterMergeClusterCapacityPolicy", 10, policy.ToUpdateScript())
                    });

                if (capacityPolicyChange != null)
                {
                    changeSet.Changes.Add(capacityPolicyChange);
                }
            }

            changeSet.Scripts.AddRange(changeSet.Changes.SelectMany(c => c.Scripts));

            // Run Kusto code diagnostics
            foreach (var script in changeSet.Scripts)
            {
                var code = KustoCode.Parse(script.Text);
                var diagnostics = code.GetDiagnostics();
                script.IsValid = diagnostics.Any() == false;
            }
            return changeSet;
        }

        /// <summary>
        /// Compares two policy objects and returns a detailed change object.
        /// </summary>
        private static IChange? ComparePolicy<T>(string entityType, string entityName, T? oldPolicy, T newPolicy, Func<T, List<DatabaseScriptContainer>> scriptGenerator) where T : class
        {
            if (newPolicy == null) return null;
            if (newPolicy.Equals(oldPolicy)) return null;

            var policyChange = new PolicyChange<T>(entityType, entityName, oldPolicy!, newPolicy);

            var changedProperties = new List<string>();
            var properties = typeof(T).GetProperties().Where(p => p.CanRead && p.CanWrite);
            
            foreach (var prop in properties)
            {
                var oldValue = oldPolicy != null ? prop.GetValue(oldPolicy) : null;
                var newValue = prop.GetValue(newPolicy);

                if (newValue != null && !object.Equals(oldValue, newValue))
                {
                    var oldValueStr = oldValue?.ToString() ?? "Not Set";
                    var newValueStr = newValue.ToString()!;
                    changedProperties.Add($"- **{prop.Name}**: `{oldValueStr}` → `{newValueStr}`");
                }
            }

            if (changedProperties.Any())
            {
                policyChange.Markdown = $"## {entityType} Changes\n\n{string.Join("\n", changedProperties)}";
                policyChange.Scripts.AddRange(scriptGenerator(newPolicy));
                return policyChange;
            }

            return null;
        }
    }
}