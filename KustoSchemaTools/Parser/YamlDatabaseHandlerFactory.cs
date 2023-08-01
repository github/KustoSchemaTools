using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser
{
    public class YamlDatabaseHandlerFactory
    {
        
        public List<IYamlSchemaPlugin> Plugins { get; } = new List<IYamlSchemaPlugin> ();

        public YamlDatabaseHandlerFactory WithPlugin(IYamlSchemaPlugin plugin)
        {
            Plugins.Add(plugin);
            return this;
        }
        public YamlDatabaseHandlerFactory WithPlugin<T>() where T: IYamlSchemaPlugin, new()
        {
            Plugins.Add(new T());
            return this;
        }

        public IDatabaseHandler Create(string deployment, string database)
        {
            return new YamlDatabaseHandler(deployment, database, Plugins);
        }
    }
}
