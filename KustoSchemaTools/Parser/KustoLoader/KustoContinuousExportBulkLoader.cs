using KustoSchemaTools.Model;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoContinuousExportBulkLoader : KustoBulkEntityLoader<ContinuousExport>
    {
        const string LoadContinuousExports = @".show database hydro cslschema script 
| parse-where  DatabaseSchemaScript with '.create-or-alter continuous-export ' EntityName:string ' to table ' ExternalTable:string ' with (forcedLatency=time(' ForcedLatency:timespan '), intervalBetweenRuns=time('IntervalBetweenRuns:timespan '), sizeLimit='SizeLimit:long', distributed='Distributed:bool', managedIdentity='ManagedIdentity:string') <| ' Query:string
| project EntityName=trim("" "",EntityName), Body = bag_pack(
    'ExternalTable', ExternalTable,
    'ForcedLatencyInMinutes',toint(ForcedLatency /1m),
    'IntervalBetweenRuns',toint(IntervalBetweenRuns /1m),
    'SizeLimit',SizeLimit,
    'Distributed', Distributed,
    'ManagedIdentity',ManagedIdentity,
    'Query',Query)";

        public KustoContinuousExportBulkLoader() : base(d => d.ContinuousExports) { }

        protected override IEnumerable<string> EnumerateScripts()
        {
            yield return LoadContinuousExports;
        }
    }
}
