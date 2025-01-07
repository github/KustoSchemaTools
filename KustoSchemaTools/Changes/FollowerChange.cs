using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoSchemaTools.Changes
{
    public class BasicChange : IChange
    {
        
        public string EntityType { get; set; }

        public BasicChange(string entityType, string entity, string markdown, List<DatabaseScriptContainer> scripts)
        {
            EntityType = entityType;
            Entity = entity;
            Markdown = markdown;
            Scripts = scripts;
        }

        public string Entity { get; set; }

        public string Markdown { get; set; }

        public List<DatabaseScriptContainer> Scripts { get; set; }

    }
}
