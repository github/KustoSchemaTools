namespace KustoSchemaTools.Changes
{
    public interface IChange
    {
        string EntityType { get; }
        string Entity { get; }

        public List<DatabaseScriptContainer> Scripts { get; }

        public string Markdown { get; }

    }

}
