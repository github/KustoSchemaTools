namespace KustoSchemaTools.Changes
{
    public class ClusterChange
    {
        public string ClusterName { get; set; }
        public PolicyChange CapacityPolicyChange { get; set; }
        // TODO: Future extensibility for workload groups can be added here
    }
}