using KustoSchemaTools.Model;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoExternalTableBulkLoader : KustoBulkEntityLoader<ExternalTable>
    {
        const string LoadExternalTables = ".show external tables | extend Properties = parse_json(Properties), ConnectionString=tostring(parse_json(ConnectionStrings)[0]) | project EntityName = TableName, Folder, DocString, Kind = case(tolower(TableType)  == \"sql\", \"sql\", tolower(TableType)  ==\"Delta\", \"delta\", \"storage\"), DataFormat = tolower(tostring(Properties.Format)), FileExtentions = tostring(Properties.FileExtension), IncludeHeaders = tostring(Properties.IncludeHeaders), Encoding = tostring(Properties.Encoding), NamePrefix = tostring(Properties.NamePrefix), Compressed = tobool(Properties.Compressed), SqlTable = tostring(Properties.TargetEntityName), CreateIfNotExists = tobool(Properties. CreateIfNotExists), PrimaryKey = tostring(Properties.PrimaryKey), SqlDialect = tostring(Properties.SqlDialect), Properties | project EntityName, Body = bag_pack_columns(Folder, DocString, Kind, DataFormat,FileExtentions, IncludeHeaders, Encoding, NamePrefix, Compressed, SqlTable, CreateIfNotExists, PrimaryKey, SqlDialect, Properties)";
        const string LoadExternalTableAdditionalData = ".show database schema as csl script | where DatabaseSchemaScript contains \".create external table\" or DatabaseSchemaScript contains \".create-or-alter external table\" | parse DatabaseSchemaScript with * \"pathformat = \" PathFormat:string \"\\n\"* | parse DatabaseSchemaScript with * \"partition by \" Partitions:string \"\\n\"* | parse DatabaseSchemaScript with *\"h@\\\"\" ConnectionString:string \"\\\"\"* | parse DatabaseSchemaScript with * \".create\" * \"external table \" Table:string \" (\" Columns:string \")\"* | mv-apply S=split(Columns,\",\") to typeof(string) on (extend C = split(S, ':') | extend B=bag_pack(trim('\\\\W',tostring(C[0])), C[1]) | summarize Schema=make_bag(B)) | extend  Partitions = trim(\"[\\\\(\\\\)\\\\r]\",Partitions), PathFormat = trim(\"\\\\)\",trim(\"\\\\r\",trim(\"\\\\(\",PathFormat))) | project EntityName=Table, Body= bag_pack_columns(Schema, ConnectionString, Partitions, PathFormat)";

        public KustoExternalTableBulkLoader() : base(d => d.ExternalTables) { }

        protected override IEnumerable<string> EnumerateScripts()
        {
            yield return LoadExternalTables;
            yield return LoadExternalTableAdditionalData;

        }
    }
}
