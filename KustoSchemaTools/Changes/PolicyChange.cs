using KustoSchemaTools.Model;
using System.Collections.Generic;

namespace KustoSchemaTools.Changes
{
    public class PolicyChange<T> : BaseChange<T>
    {
        public List<PropertyChange> PropertyChanges { get; } = new List<PropertyChange>();

        public PolicyChange(string entityType, string entity, T from, T to)
            : base(entityType, entity, from, to)
        {
        }
    }
}