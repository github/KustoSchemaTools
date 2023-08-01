namespace KustoSchemaRollout.Model
{
    public class Clusters
    {
        public List<Cluster> Connections { get; set; } = new List<Cluster>();
        public List<DatabaseScript> Scripts { get; set; } = new List<DatabaseScript>();
    }

}
