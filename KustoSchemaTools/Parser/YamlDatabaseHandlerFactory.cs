using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser
{
    public class YamlDatabaseHandlerFactory<T> where T : Database, new()
    {
        public List<IYamlSchemaPlugin<T>> Plugins { get; } = new List<IYamlSchemaPlugin<T>> ();

        public virtual YamlDatabaseHandlerFactory<T> WithPlugin(IYamlSchemaPlugin<T> plugin)
        {
            Plugins.Add(plugin);
            return this;
        }
        public virtual YamlDatabaseHandlerFactory<T> WithPlugin<TPlugin>() where TPlugin : IYamlSchemaPlugin<T>, new()
        {
            Plugins.Add(new TPlugin());
            return this;
        }

        public virtual IDatabaseHandler<T> Create(string deployment, string database)
        {
            return new YamlDatabaseHandler<T>(deployment, database, Plugins);
        }
    }
}
