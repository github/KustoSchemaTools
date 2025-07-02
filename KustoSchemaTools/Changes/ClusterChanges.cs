using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Kusto.Language;

namespace KustoSchemaTools.Changes
{
    public class ClusterChanges
    {
        /// <summary>
        /// Compares two cluster configurations and generates a comprehensive change set
        /// containing the differences and the scripts needed to apply those changes.
        /// </summary>
        /// <param name="oldCluster">The current/existing cluster configuration (typically from live cluster).</param>
        /// <param name="newCluster">The desired cluster configuration (typically from YAML file).</param>
        /// <param name="log">Logger instance for recording the comparison process and results.</param>
        /// <returns>
        /// A ClusterChangeSet containing:
        /// - Detailed policy changes with before/after values
        /// - Generated Kusto scripts to apply the changes
        /// - Validation results for each script
        /// </returns>
        /// <exception cref="ArgumentException">Thrown when cluster names don't match between old and new configurations.</exception>
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
                script.IsValid = !diagnostics.Any();
            }
            return changeSet;
        }

        /// <summary>
        /// Compares two policy objects of the same type using reflection to detect property-level changes.
        /// Only properties that are non-null in the new policy and differ from the old policy are considered changes.
        /// This approach aligns with Kusto's `.alter-merge` command behavior, which only modifies specified properties.
        /// </summary>
        /// <typeparam name="T">The type of policy object to compare (must be a reference type).</typeparam>
        /// <param name="entityType">The display name of the entity type for documentation purposes (e.g., "Cluster Capacity Policy").</param>
        /// <param name="entityName">The name of the specific entity instance being compared (e.g., "default").</param>
        /// <param name="oldPolicy">The existing policy configuration, or null if no policy was previously set.</param>
        /// <param name="newPolicy">The desired policy configuration to compare against the old policy.</param>
        /// <param name="scriptGenerator">A function that generates the Kusto scripts needed to apply the new policy.</param>
        /// <returns>
        /// An IChange object containing:
        /// - Property-level change details in Markdown format
        /// - Generated scripts to apply the changes
        /// Returns null if no meaningful changes are detected.
        /// </returns>
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