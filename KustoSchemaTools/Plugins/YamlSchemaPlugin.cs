using KustoSchemaTools.Model;

namespace KustoSchemaTools.Plugins
{
    public abstract class YamlSchemaPlugin : IYamlSchemaPlugin<Database>
    {
        public abstract Task OnLoad(Database existingDatabase, string basePath);

        public abstract Task OnWrite(Database existingDatabase, string basePath);
    }
}