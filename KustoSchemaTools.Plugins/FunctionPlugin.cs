using Kusto.Language;
using KustoSchemaRollout.Model;

namespace KustoSchemaTools.Plugins
{
    public class FunctionPlugin : EntityPlugin<Function>
    {
        public FunctionPlugin(string subFolder = "functions", int minRowLength = 5) : base(db => db.Functions, subFolder, minRowLength)
        {
        }
    }
}