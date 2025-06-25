using System.Collections.Generic;

namespace KustoSchemaTools.Changes
{
    public class PolicyChange
    {
        public List<PropertyChange> PropertyChanges { get; } = new List<PropertyChange>();
        public string UpdateScript { get; set; }
    }
}