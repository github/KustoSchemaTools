using KustoSchemaTools.Model;

namespace KustoSchemaTools.Plugins
{
    public class FollowerPlugin : EntityPlugin<FollowerDatabase>
    {
        public FollowerPlugin(string subFolder = "followers", int minRowLength = 5) : base(db => db.Followers, subFolder, minRowLength)
        {
        }
    }
}