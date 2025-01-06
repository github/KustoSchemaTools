namespace KustoSchemaTools.Model
{
    public class FollowerDatabase 
    {
        public required string DatabaseName { get; set; }
        public FollowerCache Cache { get; set; } = new FollowerCache();        
        public FollowerPermissions Permissions { get; set; } = new FollowerPermissions();
    }

}
