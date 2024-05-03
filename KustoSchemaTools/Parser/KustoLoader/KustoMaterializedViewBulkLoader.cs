using KustoSchemaTools.Model;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoMaterializedViewBulkLoader : KustoBulkEntityLoader<MaterializedView>
    {
        const string LoadMaterializedViews = ".show materialized-views  details| project EntityName=MaterializedViewName, Body=bag_pack(\"DocString\", DocString, \"Folder\", Folder,\"RetentionAndCachePolicy\",bag_pack(\"Retention\",strcat(toint(totimespan(parse_json(RetentionPolicy).SoftDeletePeriod)/1d),\"d\") , \"HotCache\", strcat(toint(totimespan(parse_json(CachingPolicy).DataHotSpan)/1d),\"d\")))";
        const string LoadDetails = ".show materialized-views | extend Lookback = strcat(toint(Lookback / 1d),\"d\") | extend Lookback = iff(Lookback  == 'd', \"\", Lookback), Source= SourceTable | project EntityName=Name, Body=bag_pack_columns(Source, Query,IsEnabled, Folder,DocString, AutoUpdateSchema, Lookback)";
        const string LoadRowLevelSecurity = ".show database schema as csl script | parse-where DatabaseSchemaScript with \".alter materialized-view \" TableName:string \" policy row_level_security enable \" Policy:string | project TableName, RowLevelSecurity = trim(\"( |\\\\\\\")*\",Policy) | project EntityName = TableName, Body=bag_pack_columns(RowLevelSecurity)";

        public KustoMaterializedViewBulkLoader() : base(d => d.MaterializedViews) { }

        protected override IEnumerable<string> EnumerateScripts()
        {
            yield return LoadMaterializedViews;
            yield return LoadDetails;
            yield return LoadRowLevelSecurity;
        }
    }
}
