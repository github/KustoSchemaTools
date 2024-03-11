using KustoSchemaTools.Model;

namespace KustoSchemaTools.Plugins
{
    public class ContinuousExportPlugin : EntityPlugin<ContinuousExport>
    {
        public ContinuousExportPlugin(string subFolder = "continuous-exports", int minRowLength = 5) : base(db => db.ContinuousExports, subFolder, minRowLength)
        {
        }
    }
}