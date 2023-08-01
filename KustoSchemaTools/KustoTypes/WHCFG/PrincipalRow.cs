using KustoSchemaTools.Model;

namespace KustoSchemaTools.KustoTypes.WHCFG
{
    public class PrincipalRow
    {
        public string Role { get; set; }
        public List<AADObject> Users { get; set; }

    }
}
