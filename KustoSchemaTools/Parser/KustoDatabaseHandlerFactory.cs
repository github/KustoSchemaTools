using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Parser
{
    public class KustoDatabaseHandlerFactory
    {
        public KustoDatabaseHandlerFactory(ILogger<KustoDatabaseHandler> logger)
        {
            Logger = logger;
        }

        public List<IKustoBulkEntitiesLoader> Reader { get; } = new ();
        public List<IDBEntityWriter> Writer { get; } = new ();

        public KustoDatabaseHandlerFactory WithPlugin(IKustoBulkEntitiesLoader plugin)
        {
            Reader.Add(plugin);
            return this;
        }

        public KustoDatabaseHandlerFactory WithReader<T>() where T : IKustoBulkEntitiesLoader, new()
        {
            Reader.Add(new T());
            return this;
        }
        public KustoDatabaseHandlerFactory WithPlugin(IDBEntityWriter plugin)
        {
            Writer.Add(plugin);
            return this;
        }

        public KustoDatabaseHandlerFactory WithWriter<T>() where T : IDBEntityWriter, new()
        {
            Writer.Add(new T());
            return this;
        }

        public ILogger<KustoDatabaseHandler> Logger { get; set; }

        public IDatabaseHandler Create(string cluster, string database)
        {
            return new KustoDatabaseHandler(cluster, database, Logger, Reader, Writer);
        }

    }

    
}
