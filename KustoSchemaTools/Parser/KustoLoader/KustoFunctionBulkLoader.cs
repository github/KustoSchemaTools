using KustoSchemaRollout.Model;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoFunctionBulkLoader : KustoBulkEntityLoader<Function>
    {
        const string LoadFunctions= ".show functions | extend Body = trim(\"[{} \\r\\n]*\", Body) | extend Parameters = trim(\"[()]\", Parameters) | project EntityName=Name, Body = bag_pack_columns(Parameters, Body, Folder,DocString)";
     
        public KustoFunctionBulkLoader() : base(d => d.Functions) { }

        protected override IEnumerable<string> EnumerateScripts()
        {
            yield return LoadFunctions;
        }
    }
}
