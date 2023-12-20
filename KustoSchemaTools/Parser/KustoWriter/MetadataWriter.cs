using Kusto.Data.Common;
using KustoSchemaTools.Helpers;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace KustoSchemaTools.Parser.KustoWriter
{
    public class MetadataWriter : IDBEntityWriter
    {
        public MetadataWriter(string table, string folder)
        {
            Table = table;
            Folder = folder;
        }

        public string Table { get; }
        public string Folder { get; }

        public async Task WriteAsync(Database sourceDb, Database targetDb, KustoClient client, ILogger logger)
        {
            if (sourceDb.Metadata?.Any() != true) return;

            var state = JsonConvert.SerializeObject(sourceDb.Metadata, Serialization.JsonPascalCase);

            var cmd = $".set-or-append {Table} with (folder='{Folder}') <| print Timestamp = now(), Metadata = todynamic(```{state}```)| mv-expand Metadata | evaluate bag_unpack(Metadata)";

            await client.AdminClient.ExecuteControlCommandAsync(targetDb.Name, cmd, new ClientRequestProperties());
        }

    }
}
