using KustoSchemaTools.Model;

namespace KustoSchemaTools.Plugins
{
    public class FollowersPlugin : EntityPlugin<FollowerDatabase>
    {
        public FollowersPlugin(string subFolder = "followers", int minRowLength = 5) : base(db => db.Followers, subFolder, minRowLength)
        {
        }
    }
}