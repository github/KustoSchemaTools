namespace KustoSchemaTools.Model
{
    public class Cluster
    {
        public string Name { get; set; }
        public string Url { get; set; }
        public List<DatabaseScript> Scripts { get; set; } = new List<DatabaseScript>();
        public ClusterCapacityPolicy? CapacityPolicy { get; set; }
        public List<WorkloadGroup> WorkloadGroups { get; set; } = new List<WorkloadGroup>();
    }

}
