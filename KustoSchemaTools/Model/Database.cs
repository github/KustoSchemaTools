namespace KustoSchemaTools.Model
{
    public class Database
    {
        public string Name { get; set; }
        public string Team { get; set; } = "";
        public RetentionAndCachePolicy DefaultRetentionAndCache { get; set; } = new RetentionAndCachePolicy();

        public List<AADObject> Monitors { get; set; } = new List<AADObject>();

        public List<AADObject> Viewers { get; set; } = new List<AADObject>();
        public List<AADObject> UnrestrictedViewers { get; set; } = new List<AADObject>();
        public List<AADObject> Users { get; set; } = new List<AADObject>();
        public List<AADObject> Ingestors { get; set; } = new List<AADObject>();
        public List<AADObject> Admins { get; set; } = new List<AADObject>();

        public Dictionary<string, FollowerDatabase> Followers { get; set; } = new Dictionary<string, FollowerDatabase>();
        public Dictionary<string, Table> Tables { get; set; } = new Dictionary<string, Table>();

        public Dictionary<string, MaterializedView> MaterializedViews { get; set; } = new Dictionary<string, MaterializedView>();

        public Dictionary<string, Function> Functions { get; set; } = new Dictionary<string, Function>();
        public Dictionary<string, ContinuousExport> ContinuousExports { get; set; } = new Dictionary<string, ContinuousExport>();

        public List<DatabaseScript> Scripts { get; set; } = new List<DatabaseScript>();

        public Dictionary<string, List<Entity>> EntityGroups { get; set; } = new Dictionary<string, List<Entity>>();

        public Dictionary<string, ExternalTable> ExternalTables { get;  set; } = new Dictionary<string, ExternalTable>();

        public List<Metadata> Metadata { get; set; } = new List<Metadata> { };

        public Deletions Deletions { get; set; } = new Deletions();

    }

}
