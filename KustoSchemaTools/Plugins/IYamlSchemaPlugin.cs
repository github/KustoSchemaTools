using KustoSchemaTools.Model;

namespace KustoSchemaTools.Plugins
{
    public interface IYamlSchemaPlugin<in T> where T : Database
    {
        Task OnLoad(T existingDatabase, string basePath);
        Task OnWrite(T existingDatabase, string basePath);
    }
}