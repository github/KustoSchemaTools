using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace KustoSchemaTools.Changes
{
    public class ClusterChanges
    {
        public static List<ClusterPolicyChange> GenerateChanges(Cluster oldCluster, Cluster newCluster, ILogger log)
        {
            var changes = new List<ClusterPolicyChange>();
            bool policiesAreDifferent = false;

            // Case 1: A new policy is defined in the YAML.
            if (newCluster.CapacityPolicy != null)
            {
                // If there was no old policy, it's automatically a change.
                if (oldCluster.CapacityPolicy == null)
                {
                    policiesAreDifferent = true;
                }
                else
                {
                    // Use reflection to compare ONLY the properties set in the new policy.
                    var newPolicyProps = newCluster.CapacityPolicy.GetType().GetProperties()
                        .Where(p => p.GetValue(newCluster.CapacityPolicy) != null);

                    foreach (var prop in newPolicyProps)
                    {
                        var newValue = prop.GetValue(newCluster.CapacityPolicy);
                        var oldValue = prop.GetValue(oldCluster.CapacityPolicy);
                        if (!object.Equals(newValue, oldValue))
                        {
                            policiesAreDifferent = true;
                            break; // Found a difference, no need to check further.
                        }
                    }
                }
            }
            // Case 2: The new policy is NOT defined in YAML, but an old one exists on the cluster (a deletion).
            else if (oldCluster.CapacityPolicy != null)
            {
                policiesAreDifferent = true;
            }

            if (policiesAreDifferent)
            {
                log.LogInformation("Cluster capacity policy has changed.");
                changes.Add(new ClusterPolicyChange
                {
                    OldPolicy = oldCluster.CapacityPolicy,
                    NewPolicy = newCluster.CapacityPolicy
                });
            }

            return changes;
        }
    }
}