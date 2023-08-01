namespace KustoSchemaTools.Changes
{
    public abstract class BaseChange<T> : IChange
    {

        protected BaseChange(string entityType, string entity, T? from, T to)
        {
            EntityType = entityType;
            Entity = entity;
            From = from;
            To = to;
        }

        public string EntityType { get; set; }
        public string Entity { get; set; }

        public T? From { get; set; }
        public T To { get; set; }

        public List<DatabaseScriptContainer> Scripts { get; set; } = new List<DatabaseScriptContainer>();


        public string Markdown { get; protected set; }

    }

}
