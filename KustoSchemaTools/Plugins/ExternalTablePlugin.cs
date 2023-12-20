using KustoSchemaTools.Model;

namespace KustoSchemaTools.Plugins
{
    public class ExternalTablePlugin : EntityPlugin<ExternalTable>
    {
        public ExternalTablePlugin(string subFolder = "external-tables", int minRowLength = 5) : base(db => db.ExternalTables, subFolder, minRowLength)
        {
        }
    }
}