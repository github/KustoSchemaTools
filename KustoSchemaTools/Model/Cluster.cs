namespace KustoSchemaRollout.Model
{
    public class Cluster
    {
        public string Name { get; set; }
        public string Url { get; set; }
        public List<DatabaseScript> Scripts { get; set; } = new List<DatabaseScript>();
    }

}
