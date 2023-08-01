using KustoSchemaTools.Model;

namespace KustoSchemaTools.KustoTypes.DB
{
    public class TableRows
    {
        public string TableName { get; set; }
        public string DocString { get; set; }
        public string Folder { get; set; }
        public List<UpdatePolicy> UpdatePolicies { get; set; }
        public RetentionAndCachePolicy RetentionAndCachePolicy { get; set; }
        public bool RestrictedViewAccess { get; set; }
        public string RowLevelSecurity { get; set; }
        public Dictionary<string, string> Columns { get; set; }

    }
}
