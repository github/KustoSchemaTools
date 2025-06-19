using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using KustoSchemaTools.Parser;
using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class Table : IKustoBaseEntity
    {

        public string Folder { get; set; }
        [Obsolete("Use policies instead")]
        public RetentionAndCachePolicy RetentionAndCachePolicy { get; set; } = new RetentionAndCachePolicy();

        public Dictionary<string, string> Columns { get; set; }
        [Obsolete("Use policies instead")]
        public List<UpdatePolicy> UpdatePolicies { get; set; }
        public TablePolicy? Policies { get; set; }
        public List<DatabaseScript> Scripts { get; set; }
        public string DocString { get; set; }
        [Obsolete("Use policies instead")]
        public string? RowLevelSecurity { get; set; }
        [Obsolete("Use policies instead")]
        public bool RestrictedViewAccess { get; set; } = false;

        /// <summary>
        /// Creates scripts for this table with optional update policy validation.
        /// </summary>
        /// <param name="name">The table name</param>
        /// <param name="isNew">Whether this is a new table</param>
        /// <param name="database">The database context (optional, for update policy validation)</param>
        /// <param name="validateUpdatePolicies">Whether to validate update policies before creating scripts</param>
        /// <returns>List of database script containers</returns>
        public List<DatabaseScriptContainer> CreateScripts(string name, bool isNew, Database? database = null, bool validateUpdatePolicies = false)
        {
            var scripts = new List<DatabaseScriptContainer>();
            if (Columns != null)
            {
                var properties = string.Join(", ", GetType().GetProperties()
                    .Where(p => p.GetValue(this) != null && (p.Name == "Folder" || p.Name == "DocString"))
                    .Select(p => $"{p.Name}=```{p.GetValue(this)}```"));

                scripts.Add(new DatabaseScriptContainer("CreateMergeTable", 30, $".create-merge table {name} ({string.Join(", ", Columns.Select(c => $"{c.Key.BracketIfIdentifier()}:{c.Value}"))})"));
            }

            scripts.Add(new DatabaseScriptContainer("TableFolder", 31, $".alter table {name} folder '{Folder}'"));
            scripts.Add(new DatabaseScriptContainer("TableDocString", 31, $".alter table {name} docstring '{DocString}'"));
           

            if (Policies != null)
            {
                scripts.AddRange(Policies.CreateScripts(name, this, database, validateUpdatePolicies));
            }
            if (Scripts != null)
            {
                scripts.AddRange(Scripts.Select(itm => new DatabaseScriptContainer(itm, "DatabaseScript")));
            }
        
            return scripts;
        }

        /// <summary>
        /// Creates scripts for this table (backward compatibility).
        /// </summary>
        /// <param name="name">The table name</param>
        /// <param name="isNew">Whether this is a new table</param>
        /// <returns>List of database script containers</returns>
        public List<DatabaseScriptContainer> CreateScripts(string name, bool isNew)
        {
            return CreateScripts(name, isNew, null, false);
        }
    }



}
