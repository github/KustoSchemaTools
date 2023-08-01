using KustoSchemaTools.Model;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoMaterializedViewBulkLoader : KustoBulkEntityLoader<MaterializedView>
    {
        const string LoadMaterializedViews = ".show materialized-views  details| project EntityName=MaterializedViewName, Body=bag_pack(\"DocString\", DocString, \"Folder\", Folder,\"RetentionAndCachePolicy\",bag_pack(\"Retention\",strcat(toint(totimespan(parse_json(RetentionPolicy).SoftDeletePeriod)/1d),\"d\") , \"HotCache\", strcat(toint(totimespan(parse_json(CachingPolicy).DataHotSpan)/1d),\"d\")))";
        const string LoadDetails = ".show materialized-views | extend Lookback = strcat(toint(Lookback / 1d),\"d\") | extend Lookback = iff(Lookback  == 'd', \"\", Lookback) | project EnitityName=Name, Body=bag_pack_columns(Source= SourceTable,Query,IsEnabled, Folder,DocString, AutoUpdateSchema, Lookback)";

        public KustoMaterializedViewBulkLoader() : base(d => d.MaterializedViews) { }

        protected override IEnumerable<string> EnumerateScripts()
        {
            yield return LoadMaterializedViews;
            yield return LoadDetails;
        }
    }
}
