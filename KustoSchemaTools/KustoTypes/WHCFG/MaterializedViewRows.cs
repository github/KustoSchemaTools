using KustoSchemaTools.Model;

namespace KustoSchemaTools.KustoTypes.WHCFG
{
    public class MaterializedViewRows
    {
        public string Name { get; set; }
        public string Source { get; set; }
        public string Query { get; set; }
        public bool IsEnabled { get; set; }
        public string Folder { get; set; }
        public string DocString { get; set; }
        public bool AutoUpdateSchema { get; set; }
        public string Lookback { get; set; }
        public RetentionAndCachePolicy RetentionAndCachePolicy { get; set; }
    }

}
