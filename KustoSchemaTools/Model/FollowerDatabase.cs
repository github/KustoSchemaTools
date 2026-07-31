namespace KustoSchemaTools.Model
{
    public class FollowerDatabase 
    {
        public string DatabaseName { get; set; }
        public FollowerCache Cache { get; set; } = new FollowerCache();
        // TODO: No logic to load data / roll out changes implemented yet!
        public FollowerPermissions Permissions { get; set; } = new FollowerPermissions();

        // True when follower metadata was returned by .show follower database
        public bool IsFollower { get; set; }

        // Populated when available from follower metadata (e.g., Data Share followers)
        public string? LeaderClusterMetadataPath { get; set; }
    }

}
