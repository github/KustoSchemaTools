using KustoSchemaRollout.Model;

namespace KustoSchemaTools.Plugins
{
    public interface IYamlSchemaPlugin
    {
        Task OnLoad(Database existingDatabase, string basePath);
        Task OnWrite(Database existingDatabase, string basePath);
    }
}