namespace KustoSchemaTools.Model
{
    public class FollowerDatabase 
    {
        public string DatabaseName { get; set; }
        public FollowerCache Cache { get; set; } = new FollowerCache();
        // TODO: No logic to load data / roll out changes implemented yet!
        public FollowerPermissions Permissions { get; set; } = new FollowerPermissions();
    }

}
