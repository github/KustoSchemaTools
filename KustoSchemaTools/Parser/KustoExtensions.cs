using Kusto.Ingest;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Data;
using Kusto.Language.Editor;
using Kusto.Language;

namespace KustoSchemaTools.Parser
{
    public static class KustoExtensions
    {
        public static JsonSerializerSettings DefaultJsonSettings = new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Ignore,
            Formatting = Formatting.Indented,
            ContractResolver = new Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver()
            

        };

        public static string GetTableSasUri(this IKustoIngestionResult result)
        {
            var t1 = result.GetType();
            var p1 = t1.GetProperty("IngestionStatusTable");
            var o1 = p1.GetValue(result);

            var t2 = o1.GetType();
            var p2 = t2.GetProperty("TableSasUri");
            var o2 = p2.GetValue(o1);

            return o2.ToString();
        }

        public static List<T> As<T>(this IDataReader reader)
        {
            var table = new DataTable();
            table.Load(reader);
            return As<T>(table);
        }

        public static JObject AsDynamic(this IDataReader reader)
        {
            var table = new DataTable();
            table.Load(reader);
            return AsDynamic(table);
        }

        public static T ToScalar<T>(this IDataReader reader)
        {
            if (reader.Read())
            {
                return (T)reader.GetValue(0);
            }
            throw new InvalidOperationException("Can't get the value from the resultset, because it is empty.");
        }

        public static List<T> As<T>(this DataTable table)
        {
            var json = JsonConvert.SerializeObject(table);
            return JsonConvert.DeserializeObject<List<T>>(json, DefaultJsonSettings);
        }

        public static JObject AsDynamic(this DataTable table)
        {
            var json = JsonConvert.SerializeObject(table);
            return JObject.Parse(json);
        }

        public static string ToKustoClusterUrl(this string cluster, bool ingest = false)
        {
            var ingestPrefix = ingest ? "ingest-" : "";
            return cluster.StartsWith("https") ? cluster : $"https://{ingestPrefix}{cluster}.kusto.windows.net";
        }

        
        public static string PrettifyKql(this string query)
        {
            return new KustoCodeService(KustoCode.Parse(query)).GetFormattedText().Text.Replace("\r", "").Replace("\n\n", "\n");
        }
        
        public static string UseHtmlLineBreaks(this string query)
        {
            return query.Replace("\r", "").Replace("\n", "<br>");
        }

        


    }
}

