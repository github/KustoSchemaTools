namespace KustoSchemaTools.Model
{
    public class FollowerPermissions
    {
        public FollowerModificationKind ModificationKind { get; set; }
        public List<AADObject> Viewers { get; set; } = new List<AADObject>();
        public List<AADObject> Admins { get; set; } = new List<AADObject>();
    }

}
