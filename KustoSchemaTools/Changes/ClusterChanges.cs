using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Linq;

namespace KustoSchemaTools.Changes
{
    public class ClusterChanges
    {
        public static ClusterChange GenerateChanges(Cluster oldCluster, Cluster newCluster,  ILogger log)
        {
            if (oldCluster.name != newCluster.name) {
                throw new ArgumentException($"Cluster names must match; {oldCluster.Name} != {newCluster.Name}");
            }
            
            var clusterName = oldCluster.Name;
            var clusterChange = new ClusterChange
            {
                ClusterName = clusterName
            };

            if (newCluster.CapacityPolicy != null)
            {
                log.LogInformation($"Analyzing capacity policy changes for cluster {clusterName}...");

                var capacityPolicyChange = new PolicyChange(); 
                var newPolicyProps = newCluster.CapacityPolicy.GetType().GetProperties()
                    .Where(p => p.GetValue(newCluster.CapacityPolicy) != null);

                foreach (var prop in newPolicyProps)
                {
                    var newValue = prop.GetValue(newCluster.CapacityPolicy);
                    var oldValue = prop.GetValue(oldCluster.CapacityPolicy);

                    if (!object.Equals(newValue, oldValue))
                    {
                        capacityPolicyChange.PropertyChanges.Add(new PropertyChange
                        {
                            PropertyName = prop.Name,
                            OldValue = oldValue?.ToString() ?? "Not Set",
                            NewValue = newValue.ToString()
                        });
                    }
                }

                if (capacityPolicyChange.PropertyChanges.Any())
                {
                    log.LogInformation($"{capacityPolicyChange.PropertyChanges.Count} properties in the capacity policy have changed.");
                    var newPolicyJson = JsonConvert.SerializeObject(newCluster.CapacityPolicy, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
                    capacityPolicyChange.UpdateScript = $".alter-merge cluster policy capacity @'{newPolicyJson}'";
                    clusterChange.CapacityPolicyChange = capacityPolicyChange;
                }
                else
                {
                    log.LogInformation("No changes detected in the capacity policy.");
                }
            }

            return clusterChange;
        }
    }
}