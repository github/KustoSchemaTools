namespace KustoSchemaTools.Changes
{
    public class PolicyChange<T> : BaseChange<T>
    {
        public PolicyChange(string entityType, string entity, T from, T to)
            : base(entityType, entity, from, to)
        {
        }
    }
}