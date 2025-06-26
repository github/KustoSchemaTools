using KustoSchemaTools.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using YamlDotNet.Core;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace KustoSchemaTools.Helpers
{
    public static class Serialization
    {
        public static JsonSerializerSettings JsonPascalCase { get; } = new JsonSerializerSettings
        {
            Formatting = Formatting.Indented,
            ContractResolver = new PascalCaseContractResolver(),
            NullValueHandling = NullValueHandling.Ignore,
        };

        public static JsonSerializer JsonSerializer { get; } = new JsonSerializer
        {
            Formatting = Formatting.Indented,
            ContractResolver = new PascalCaseContractResolver(),
            DefaultValueHandling = DefaultValueHandling.Ignore,

        }; 
        
        public static JsonSerializer CloneJsonSerializer { get; } = new JsonSerializer
        {
            DefaultValueHandling = DefaultValueHandling.Ignore
        };


        public static ISerializer YamlPascalCaseSerializer { get; } =
            new SerializerBuilder()
                .ConfigureDefaultValuesHandling(DefaultValuesHandling.OmitDefaults | DefaultValuesHandling.OmitEmptyCollections | DefaultValuesHandling.OmitNull)
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .WithAttributeOverride<Function>(
                    c => c.Body,
                    new YamlMemberAttribute
                    {
                        ScalarStyle = ScalarStyle.Literal
                    }
                )
                .Build();
        public static IDeserializer YamlPascalCaseDeserializer { get; } =
            new DeserializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();

        // Define a custom contract resolver that uses PascalCase naming convention
        public class PascalCaseContractResolver : DefaultContractResolver
        {
            protected override string ResolvePropertyName(string propertyName)
            {
                return base.ResolvePropertyName(PascalCaseNamingConvention.Instance.Apply(propertyName));
            }
        }


        public static T Merge<T>(this T baseObject, T mergeObject)
        {
            var o1 = JObject.FromObject(baseObject, CloneJsonSerializer);
            var o2 = JObject.FromObject(mergeObject, CloneJsonSerializer);
            o1.Merge(o2, new JsonMergeSettings
            {
                MergeArrayHandling = MergeArrayHandling.Replace 
            });

            return o1.ToObject<T>(CloneJsonSerializer);
        }

        public static T Clone<T>(this T baseObject)
        {
            return JsonConvert.DeserializeObject<T>(JsonConvert.SerializeObject(baseObject));
        }

        public static int RowLength(this string source)
        {
            return source.Where(itm => itm.Equals('\n')).Count() +1;
        }

        public static void Merge<T>(this Dictionary<string, T> dict, T entity, string name) where T : new()
        {
            var existingEntity = new T();
            if (dict.ContainsKey(name))
            {
                existingEntity = dict[name];
            }
            var merged = existingEntity.Merge(entity);

            dict[name] = merged;
        }


    }

}
