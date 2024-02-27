using KustoSchemaTools.Helpers;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Plugins
{
    public abstract class EntityPlugin<T>: YamlSchemaPlugin where T : new()
    {
        public EntityPlugin(Func<Database, Dictionary<string, T>> selector, string subFolder, int minFileLength)
        {
            Selector = selector;
            SubFolder = subFolder;
            MinFileLength = minFileLength;
        }

        public Func<Database, Dictionary<string, T>> Selector { get; }
        public string SubFolder { get; }
        public int MinFileLength { get; }

        public async override Task OnLoad(Database existingDatabase, string basePath)
        {
            var dict = Selector(existingDatabase);
            var path = Path.Combine(basePath, SubFolder);

            if (Directory.Exists(path) == false) return;
            var files = Directory.GetFiles(path, "*.yml");
            foreach (var filePath in files)
            {
                var file = await File.ReadAllTextAsync(filePath);
                var entity = Serialization.YamlPascalCaseDeserializer.Deserialize<T>(file);
                var name = Path.GetFileNameWithoutExtension(filePath);

                var existingEntity = new T();
                if (dict.ContainsKey(name))
                {
                    existingEntity = dict[name];
                }
                var merged = existingEntity.Merge(entity);

                dict[name] = merged;
            }
        }

        public async override Task OnWrite(Database existingDatabase, string path)
        {
            var dict = Selector(existingDatabase);

            foreach (var entity in dict)
            {
                var yaml = Serialization.YamlPascalCaseSerializer.Serialize(entity.Value);
                if (yaml.RowLength() >= MinFileLength)
                {
                    var entitySubfolderPath = Path.Combine(path, SubFolder);
                    Directory.CreateDirectory(entitySubfolderPath);
                    await File.WriteAllTextAsync(Path.Combine(entitySubfolderPath, $"{entity.Key}.yml"), yaml);
                    dict.Remove(entity.Key);
                }
            }
        }
    }
}
