namespace KustoSchemaTools
{
    public class YamlClusterHandlerFactory
    {
        public virtual YamlClusterHandler Create(string path)
        {
            return new YamlClusterHandler(path);
        }
    }
}