using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Kusto.Language;
using System.Text;

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
            HandleCapacityPolicyChanges(oldCluster, newCluster, changeSet, log);

            log.LogInformation($"Analyzing workload group changes for cluster {clusterName}...");
            HandleWorkloadGroupChanges(oldCluster, newCluster, changeSet, log);

            changeSet.Scripts.AddRange(changeSet.Changes.SelectMany(c => c.Scripts));

            // Run Kusto code diagnostics
            foreach (var script in changeSet.Scripts)
            {
                var code = KustoCode.Parse(script.Text);
                var diagnostics = code.GetDiagnostics();
                script.IsValid = !diagnostics.Any();
            }

            changeSet.Markdown = GenerateClusterMarkdown(changeSet);

            return changeSet;
        }

        /// <summary>
        /// Formats a property value for display in markdown, handling collections and complex objects appropriately.
        /// </summary>
        /// <param name="value">The value to format.</param>
        /// <returns>A formatted string representation of the value.</returns>
        private static string FormatPropertyValue(object? value)
        {
            if (value == null) return "Not Set";
            
            // Handle collections specially
            if (value is System.Collections.IEnumerable enumerable && !(value is string))
            {
                var items = new List<string>();
                foreach (var item in enumerable)
                {
                    if (item != null)
                    {
                        var itemStr = item.ToString();
                        items.Add(itemStr!);
                    }
                }
                
                if (items.Count == 0) return "[]";
                if (items.Count == 1) return items[0];
                return $"[{string.Join(", ", items)}]";
            }
            
            return value.ToString()!;
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
                    var oldValueStr = FormatPropertyValue(oldValue);
                    var newValueStr = FormatPropertyValue(newValue);
                    changedProperties.Add($"- **{prop.Name}**: `{oldValueStr}` → `{newValueStr}`");
                }
            }

            if (changedProperties.Any())
            {
                var action = oldPolicy == null ? "Create" : "Update";
                policyChange.Markdown = $"### {action} {entityType} `{entityName}`\n\n{string.Join("\n", changedProperties)}";
                policyChange.Scripts.AddRange(scriptGenerator(newPolicy));
                return policyChange;
            }

            return null;
        }

        /// <summary>
        /// Handles capacity policy changes between old and new cluster configurations.
        /// Compares the capacity policies and adds any necessary changes to the change set.
        /// </summary>
        /// <param name="oldCluster">The current/existing cluster configuration.</param>
        /// <param name="newCluster">The desired cluster configuration.</param>
        /// <param name="changeSet">The change set to add capacity policy changes to.</param>
        /// <param name="log">Logger instance for recording the comparison process.</param>
        private static void HandleCapacityPolicyChanges(Cluster oldCluster, Cluster newCluster, ClusterChangeSet changeSet, ILogger log)
        {
            if (newCluster.CapacityPolicy == null)
            {
                log.LogInformation("No capacity policy defined in the new cluster configuration.");
            }
            else
            {
                var capacityPolicyChange = ComparePolicy(
                    "Capacity Policy",
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
        }

        /// <summary>
        /// Handles workload group changes between old and new cluster configurations.
        /// Processes both deletions and creations/updates of workload groups.
        /// </summary>
        /// <param name="oldCluster">The current/existing cluster configuration.</param>
        /// <param name="newCluster">The desired cluster configuration.</param>
        /// <param name="changeSet">The change set to add workload group changes to.</param>
        /// <param name="log">Logger instance for recording the comparison process.</param>
        private static void HandleWorkloadGroupChanges(Cluster oldCluster, Cluster newCluster, ClusterChangeSet changeSet, ILogger log)
        {
            // Handle workload group deletions first
            var workloadGroupsToDelete = newCluster.Deletions?.WorkloadGroups ?? new List<string>();
            foreach (var workloadGroupName in workloadGroupsToDelete)
            {
                var existingWorkloadGroup = oldCluster.WorkloadGroups.FirstOrDefault(wg => wg.WorkloadGroupName == workloadGroupName);
                if (existingWorkloadGroup != null)
                {
                    log.LogInformation($"Marking workload group '{workloadGroupName}' for deletion.");
                    var deletionChange = new DeletionChange(workloadGroupName, "workload_group");

                    // Replace the header in the deletion markdown
                    var originalMarkdown = deletionChange.Markdown;
                    var modifiedMarkdown = originalMarkdown.Replace($"## {workloadGroupName}", $"### Drop Workload Group {workloadGroupName}");
                    deletionChange.Markdown = modifiedMarkdown;

                    changeSet.Changes.Add(deletionChange);
                }
                else
                {
                    log.LogWarning($"Workload group '{workloadGroupName}' marked for deletion but does not exist in the live cluster.");
                }
            }

            // Handle workload group creations and updates
            foreach (var newWorkloadGroup in newCluster.WorkloadGroups)
            {
                // Skip if this workload group is marked for deletion
                if (workloadGroupsToDelete.Contains(newWorkloadGroup.WorkloadGroupName))
                {
                    log.LogInformation($"Skipping update to workload group {newWorkloadGroup.WorkloadGroupName} as it is marked for deletion.");
                    continue;
                }

                var existingWorkloadGroup = oldCluster.WorkloadGroups.FirstOrDefault(wg => wg.WorkloadGroupName == newWorkloadGroup.WorkloadGroupName);
                
                // Only create a change if the workload group policy is not null
                if (newWorkloadGroup.WorkloadGroupPolicy != null)
                {
                    var scriptType = existingWorkloadGroup == null ? "ClusterWorkloadGroupCreateOrAlterCommand" : "ClusterWorkloadGroupAlterMergeCommand";
                    var scriptText = existingWorkloadGroup == null ? newWorkloadGroup.ToCreateScript() : newWorkloadGroup.ToUpdateScript();
                    
                    var workloadGroupChange = ComparePolicy(
                        "Workload Group",
                        newWorkloadGroup.WorkloadGroupName,
                        existingWorkloadGroup?.WorkloadGroupPolicy,
                        newWorkloadGroup.WorkloadGroupPolicy,
                        wg => new List<DatabaseScriptContainer> {
                            new DatabaseScriptContainer(
                                scriptType,
                                5,
                                scriptText
                                )
                        });

                    if (workloadGroupChange != null)
                    {
                        changeSet.Changes.Add(workloadGroupChange);
                    }
                }
            }
        }

        /// <summary>
        /// Generates a markdown representation of all cluster changes.
        /// </summary>
        /// <param name="changeSet">The cluster change set containing all detected changes.</param>
        /// <returns>A formatted markdown string documenting all changes and scripts.</returns>
        private static string GenerateClusterMarkdown(ClusterChangeSet changeSet)
        {
            var sb = new StringBuilder();
            
            sb.AppendLine($"## Cluster: {changeSet.Entity}");
            sb.AppendLine();

            if (changeSet.Changes.Count == 0)
            {
                sb.AppendLine("No changes detected for this cluster.");
                return sb.ToString();
            }
            else
            {
                sb.AppendLine(":warning: **Warning**: It is strongly recommended to consult with Microsoft Support before making cluster configuration changes.");
                sb.AppendLine();

                foreach (var change in changeSet.Changes)
                {
                    if (!string.IsNullOrEmpty(change.Markdown))
                    {
                        sb.AppendLine(change.Markdown);
                    }
                    sb.AppendLine();
                }
            }

            // Add scripts section
            if (changeSet.Scripts.Count != 0)
            {
                sb.AppendLine("## Scripts to be executed:");
                sb.AppendLine("```kql");
                foreach (var script in changeSet.Scripts)
                {
                    sb.AppendLine(script.Text);
                    sb.AppendLine();
                }
                sb.AppendLine("```");
            }

            return sb.ToString();
        }
    }
}