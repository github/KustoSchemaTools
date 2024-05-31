namespace KustoSchemaTools.Model
{
    public class Deletions
    {
        public List<string> Tables { get; set; } = new List<string>();
        public List<string> Columns { get; set; } = new List<string>();
        public List<string> MaterializedViews { get; set; } = new List<string>();
        public List<string> Functions { get; set; } = new List<string>();
        public List<string> ContinuousExports { get; set; } = new List<string>();
        public List<string> ExternalTables { get; set; } = new List<string>();
    }

}
