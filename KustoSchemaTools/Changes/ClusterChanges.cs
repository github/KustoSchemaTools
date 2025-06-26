using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Linq;

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

            // 1. Get capacity policy changes
            log.LogInformation($"Analyzing capacity policy changes for cluster {clusterName}...");
            if (newCluster.CapacityPolicy == null) {
                log.LogInformation("No capacity policy defined in the new cluster configuration.");
            } else {
                var capacityPolicyChange = ComparePolicy(
                    "Cluster Capacity Policy",
                    "default",
                    oldCluster.CapacityPolicy!,
                    newCluster.CapacityPolicy!,
                    policy => new List<DatabaseScriptContainer> {
                    new DatabaseScriptContainer("AlterClusterCapacityPolicy", 10, newCluster.CapacityPolicy!.ToUpdateScript())
                    });

                if (capacityPolicyChange != null)
                {
                    changeSet.Changes.Add(capacityPolicyChange);
                }
            }

            changeSet.Scripts.AddRange(changeSet.Changes.SelectMany(c => c.Scripts));
            return changeSet;
        }

        /// <summary>
        /// Compares two policy objects property-by-property and returns a detailed change object.
        /// </summary>
        private static IChange ComparePolicy<T>(string entityType, string entityName, T oldPolicy, T newPolicy, Func<T, List<DatabaseScriptContainer>> scriptGenerator) where T : class
        {
            if (newPolicy == null) return null;

            // Create the specialized change object that can hold property-level diffs
            var policyChange = new PolicyChange<T>(entityType, entityName, oldPolicy, newPolicy);

            // Use reflection to find all changed properties
            var properties = typeof(T).GetProperties().Where(p => p.CanRead && p.CanWrite);
            foreach (var prop in properties)
            {
                var oldValue = prop.GetValue(oldPolicy);
                var newValue = prop.GetValue(newPolicy);

                // Only consider properties set in the new policy for changes
                if (newValue != null && !object.Equals(oldValue, newValue))
                {
                    policyChange.PropertyChanges.Add(new PropertyChange
                    {
                        PropertyName = prop.Name,
                        OldValue = oldValue?.ToString() ?? "Not Set",
                        NewValue = newValue.ToString()
                    });
                }
            }

            // If any properties were changed, finalize the change object and return it
            if (policyChange.PropertyChanges.Any())
            {
                policyChange.Scripts.AddRange(scriptGenerator(newPolicy));
                return policyChange;
            }

            return null;
        }
    }
}