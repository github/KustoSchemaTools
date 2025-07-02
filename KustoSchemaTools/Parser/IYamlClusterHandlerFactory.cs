namespace KustoSchemaTools.Parser
{
    public interface IYamlClusterHandlerFactory
    {
        YamlClusterHandler Create(string path);
    }
}
