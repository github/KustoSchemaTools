using KustoSchemaTools.Helpers;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser
{
    public class YamlDatabaseHandler<T> : IDatabaseHandler<T> where T : Database, new()
    {

        public YamlDatabaseHandler(string deployment, string database, List<IYamlSchemaPlugin<T>> plugins)
        {
            Deployment = deployment;
            Database = database;
            Plugins = plugins;
        }

        public string Deployment { get; }
        public string Database { get; }

        protected List<IYamlSchemaPlugin<T>> Plugins { get; }
        public virtual async Task<T> LoadAsync()
        {
            var folder = Path.Join(Deployment, Database);
            var dbFileName = Path.Join(folder, "database.yml");
            var dbYaml = File.ReadAllText(dbFileName);
            var db = Serialization.YamlPascalCaseDeserializer.Deserialize<T>(dbYaml);
            db.Name = Database;
            foreach (var plugin in Plugins)
            {
                await plugin.OnLoad(db, Path.Join(Deployment, Database));
            }
            return db;
        }

        public virtual async Task WriteAsync(T database)
        {
            var clone = database.Clone();
            var path = Path.Join(Deployment, Database);
            if(!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            foreach (var plugin in Plugins)
            {
                await plugin.OnWrite(clone, path);
            }
            var yaml = Serialization.YamlPascalCaseSerializer.Serialize(clone);
            File.WriteAllText(Path.Join(path, "database.yml"), yaml);
        }
    }
}
