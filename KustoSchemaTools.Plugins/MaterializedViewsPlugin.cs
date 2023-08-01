using KustoSchemaRollout.Model;

namespace KustoSchemaTools.Plugins
{
    public class MaterializedViewsPlugin : EntityPlugin<MaterializedView>
    {
        public MaterializedViewsPlugin(string subFolder = "materialized-views", int minRowLength = 5) : base(db => db.MaterializedViews, subFolder, minRowLength)
        {
        }
    }
}