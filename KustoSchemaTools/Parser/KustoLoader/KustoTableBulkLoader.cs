using KustoSchemaRollout.Model;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoTableBulkLoader : KustoBulkEntityLoader<Table>
    {
        const string LoadTables = ".show tables details| project TableName, DocString, Folder, RetentionAndCachePolicy=bag_pack(\"Retention\",strcat(toint(totimespan(parse_json(RetentionPolicy).SoftDeletePeriod)/1d),\"d\") , \"HotCache\",strcat(toint(totimespan(parse_json(CachingPolicy).DataHotSpan)/1d),\"d\")) | project EntityName = TableName, Body = bag_pack_columns(DocString, Folder, RetentionAndCachePolicy)";
        const string LoadUpdatePolicies = ".show database schema as csl script | parse-where DatabaseSchemaScript with '.alter table ' TableName:string ' policy update \\\"' Policy:string  '\\\"' | project TableName, UpdatePolicies = parse_json(replace_string(Policy, '\\\\\\\"','\\\"')) | project EntityName = TableName , Body = bag_pack_columns(UpdatePolicies)";
        const string LoadRestrictedViewAccess = ".show database schema as csl script | parse-where DatabaseSchemaScript with \".alter tables (\" TableName:string \") policy restricted_view_access True\" | project EntityName = TableName, Body = bag_pack(\"RestrictedViewAccess\", true)";
        const string LoadRowLevelSecurity = ".show database schema as csl script | parse-where DatabaseSchemaScript with \".alter table \" TableName:string \" policy row_level_security enable \" Policy:string | project TableName, RowLevelSecurity = trim(\"( |\\\\\\\")*\",Policy) | project EntityName = TableName, Body=bag_pack_columns(RowLevelSecurity)";
        const string LoadTableColumns = ".show database schema as csl | project TableName, Schema | extend Columns = split(Schema,\",\") | mv-apply Columns to typeof(string) on ( project ColSplit =split(Columns,\":\") | project Prop=pack(tostring(ColSplit[0]), tostring(ColSplit[1])) | summarize Columns =make_bag(Prop)) | project EntityName = TableName, Body=bag_pack_columns(Columns)";

        public KustoTableBulkLoader() : base(d => d.Tables) { }

        protected override IEnumerable<string> EnumerateScripts()
        {
            yield return LoadTables;
            yield return LoadUpdatePolicies;
            yield return LoadRestrictedViewAccess;
            yield return LoadRowLevelSecurity;
            yield return LoadTableColumns;
        }
    }
}
