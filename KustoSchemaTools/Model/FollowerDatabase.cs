namespace KustoSchemaTools.Model
{
    public class FollowerDatabase
    {
        public string Name { get; set; }
        public string Team { get; set; } = ""; 
        public string Url { get; set; }
        public List<AADObject> Viewers { get; set; } = new List<AADObject>();
        public string DatabaseHotCache { get; set; }
        public Dictionary<string,string> TablesHotCache { get; set; }
    }
}
