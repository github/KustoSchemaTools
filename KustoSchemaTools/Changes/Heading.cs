namespace KustoSchemaTools.Changes
{
    public class Heading : IChange
    {
        public Heading(string title)
        {
            Entity = title;
        }

        public string EntityType => "Heading";
        public string Entity { get; set; }

        public List<DatabaseScriptContainer> Scripts => new List<DatabaseScriptContainer> { };

        public string Markdown => $"# {Entity}";

        public string Cluster { get; }
    }

}
