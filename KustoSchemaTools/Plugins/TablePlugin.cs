using KustoSchemaTools.Model;

namespace KustoSchemaTools.Plugins
{
    public class TablePlugin : EntityPlugin<Table>
    {
        public TablePlugin(string subFolder = "tables", int minRowLength = 5) : base(db => db.Tables, subFolder, minRowLength)
        {
        }
    }
}