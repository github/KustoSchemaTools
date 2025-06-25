using KustoSchemaTools.Model;

namespace KustoSchemaTools.Changes
{
    public class ClusterPolicyChange
    {
        public string ClusterName { get; set; }
        public ClusterCapacityPolicy OldPolicy { get; set; }
        public ClusterCapacityPolicy NewPolicy { get; set; }
    }
}