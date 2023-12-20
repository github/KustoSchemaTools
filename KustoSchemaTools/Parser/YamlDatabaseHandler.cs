using KustoSchemaTools.Helpers;
using KustoSchemaTools.Model;
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
            db.Name = Database;
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
            if(!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            foreach (var plugin in Plugins)
            {
                await plugin.OnWrite(clone, path);
            }
            var yaml = Serialization.YamlPascalCaseSerializer.Serialize(clone);
            File.WriteAllText(Path.Combine(path, "database.yml"), yaml);
        }
    }
}
