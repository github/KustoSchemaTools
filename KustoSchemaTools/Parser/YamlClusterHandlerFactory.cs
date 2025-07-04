using KustoSchemaTools.Parser;

namespace KustoSchemaTools
{
    public class YamlClusterHandlerFactory : IYamlClusterHandlerFactory
    {
        public virtual YamlClusterHandler Create(string path)
        {
            return new YamlClusterHandler(path);
        }
    }
}