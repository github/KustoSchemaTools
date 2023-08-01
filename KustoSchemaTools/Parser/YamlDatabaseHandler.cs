using KustoSchemaRollout.Model;
using KustoSchemaTools.Helpers;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser
{
    public class YamlDatabaseHandler : IDatabaseHandler
    {

        public YamlDatabaseHandler(string deployment, string database, List<IYamlSchemaPlugin> plugins)
        {
            Deployment = deployment;
            Database = database;
            Plugins = plugins;
        }

        public string Deployment { get; }
        public string Database { get; }

        private List<IYamlSchemaPlugin> Plugins { get; }
        public async Task<Database> LoadAsync()
        {
            var folder = Path.Combine(Deployment, Database);
            var dbFileName = Path.Combine(folder, "database.yml");
            var dbYaml = File.ReadAllText(dbFileName);
            var db = Serialization.YamlPascalCaseDeserializer.Deserialize<Database>(dbYaml);

            foreach (var plugin in Plugins)
            {
                await plugin.OnLoad(db, Path.Combine(Deployment, Database));
            }
            return db;
        }

        public async Task WriteAsync(Database database)
        {
            var clone = database.Clone();
            var path = Path.Combine(Deployment, Database);

            foreach (var plugin in Plugins)
            {
                await plugin.OnWrite(clone, path);
            }
            var yaml = Serialization.YamlPascalCaseSerializer.Serialize(clone);
            File.WriteAllText(Path.Combine(path, "database.yml"), yaml);
        }
    }
}
