namespace KustoSchemaTools.Model
{
    public class FollowerCache
    {
        public string? DefaultHotCache { get; set; }
        public FollowerModificationKind ModificationKind { get; set; }
        public Dictionary<string, string> Tables { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> MaterializedViews { get; set; } = new Dictionary<string, string>();
    }

}
