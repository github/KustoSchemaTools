using KustoSchemaTools.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KustoSchemaTools.Tests
{
    public class ExternalTableTests
    {
        private ExternalTable CreateDeltaTable(QueryAccelerationPolicy? qa = null)
        {
            return new ExternalTable
            {
                Kind = "delta",
                ConnectionString = "https://storageaccount.blob.core.windows.net/container/path",
                DataFormat = "parquet",
                Folder = "test",
                DocString = "test table",
                QueryAcceleration = qa
            };
        }

        #region QueryAccelerationPolicy Script Generation

        [Fact]
        public void CreateScripts_DeltaWithQueryAcceleration_EmitsAlterPolicyScript()
        {
            var table = CreateDeltaTable(new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "7.00:00:00"
            });

            var scripts = table.CreateScripts("MyTable", true);

            var qaScript = scripts.Single(s => s.Kind == "QueryAccelerationPolicy");
            Assert.Contains(".alter-merge external table MyTable policy query_acceleration", qaScript.Script.Text);
            Assert.Contains("\"IsEnabled\":true", qaScript.Script.Text);
            Assert.Contains("\"Hot\":\"7.00:00:00\"", qaScript.Script.Text);
            Assert.Equal(80, qaScript.Script.Order);
        }

        [Fact]
        public void CreateScripts_DeltaWithoutQueryAcceleration_OnlyEmitsTableScript()
        {
            var table = CreateDeltaTable();

            var scripts = table.CreateScripts("MyTable", true);

            Assert.Single(scripts);
            Assert.Equal("External Table", scripts[0].Kind);
        }

        [Fact]
        public void CreateScripts_DeltaWithAllQAProperties_SerializesAllFieldsCorrectly()
        {
            var table = CreateDeltaTable(new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "30.00:00:00",
                MaxAge = "00:10:00",
                ManagedIdentity = "12345678-1234-1234-1234-1234567890ab",
                HotDateTimeColumn = "EventTimestamp",
                HotWindows = new List<HotWindow>
                {
                    new HotWindow { MinValue = "2025-07-07 07:00:00", MaxValue = "2025-07-09 07:00:00" }
                }
            });

            var scripts = table.CreateScripts("MyTable", true);
            var qaScript = scripts.Single(s => s.Kind == "QueryAccelerationPolicy");

            // Extract JSON from the command
            var text = qaScript.Script.Text;
            var jsonStart = text.IndexOf("'") + 1;
            var jsonEnd = text.LastIndexOf("'");
            var json = text.Substring(jsonStart, jsonEnd - jsonStart);
            var parsed = JObject.Parse(json);

            Assert.True(parsed["IsEnabled"]!.Value<bool>());
            Assert.Equal("30.00:00:00", parsed["Hot"]!.Value<string>());
            Assert.Equal("00:10:00", parsed["MaxAge"]!.Value<string>());
            Assert.Equal("12345678-1234-1234-1234-1234567890ab", parsed["ManagedIdentity"]!.Value<string>());
            Assert.Equal("EventTimestamp", parsed["HotDateTimeColumn"]!.Value<string>());

            var hotWindows = parsed["HotWindows"] as JArray;
            Assert.NotNull(hotWindows);
            Assert.Single(hotWindows);
            Assert.Equal("2025-07-07 07:00:00", hotWindows[0]!["MinValue"]!.Value<string>());
            Assert.Equal("2025-07-09 07:00:00", hotWindows[0]!["MaxValue"]!.Value<string>());
        }

        [Fact]
        public void CreateScripts_DeltaWithMinimalQA_OmitsNullOptionalFields()
        {
            var table = CreateDeltaTable(new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "1.00:00:00"
            });

            var scripts = table.CreateScripts("MyTable", true);
            var qaScript = scripts.Single(s => s.Kind == "QueryAccelerationPolicy");

            var text = qaScript.Script.Text;
            Assert.DoesNotContain("MaxAge", text);
            Assert.DoesNotContain("ManagedIdentity", text);
            Assert.DoesNotContain("HotDateTimeColumn", text);
            Assert.DoesNotContain("HotWindows", text);
        }

        [Fact]
        public void CreateScripts_DeltaTable_AlwaysIncludesExternalTableScript()
        {
            var table = CreateDeltaTable(new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "7.00:00:00"
            });

            var scripts = table.CreateScripts("MyTable", true);

            Assert.Equal(2, scripts.Count);
            Assert.Single(scripts, s => s.Kind == "External Table");
            Assert.Single(scripts, s => s.Kind == "QueryAccelerationPolicy");
        }

        #endregion

        #region Validation

        [Fact]
        public void CreateScripts_StorageWithQueryAcceleration_Throws()
        {
            var table = new ExternalTable
            {
                Kind = "storage",
                ConnectionString = "https://storageaccount.blob.core.windows.net/container",
                DataFormat = "csv",
                Folder = "test",
                DocString = "test",
                Schema = new Dictionary<string, string> { { "Col1", "string" } },
                QueryAcceleration = new QueryAccelerationPolicy { IsEnabled = true, Hot = "7.00:00:00" }
            };

            var ex = Assert.Throws<ArgumentException>(() => table.CreateScripts("MyTable", true));
            Assert.Contains("only supported on delta", ex.Message);
        }

        [Fact]
        public void CreateScripts_SqlWithQueryAcceleration_Throws()
        {
            var table = new ExternalTable
            {
                Kind = "sql",
                ConnectionString = "Server=tcp:server.database.windows.net",
                SqlTable = "dbo.MyTable",
                Folder = "test",
                DocString = "test",
                Schema = new Dictionary<string, string> { { "Col1", "string" } },
                QueryAcceleration = new QueryAccelerationPolicy { IsEnabled = true, Hot = "7.00:00:00" }
            };

            var ex = Assert.Throws<ArgumentException>(() => table.CreateScripts("MyTable", true));
            Assert.Contains("only supported on delta", ex.Message);
        }

        [Fact]
        public void CreateScripts_StorageWithoutQueryAcceleration_DoesNotEmitQAScript()
        {
            var table = new ExternalTable
            {
                Kind = "storage",
                ConnectionString = "https://storageaccount.blob.core.windows.net/container",
                DataFormat = "csv",
                Folder = "test",
                DocString = "test",
                Schema = new Dictionary<string, string> { { "Col1", "string" } }
            };

            var scripts = table.CreateScripts("MyTable", true);

            Assert.Single(scripts);
            Assert.Equal("External Table", scripts[0].Kind);
        }

        [Fact]
        public void QueryAccelerationPolicy_Validate_HotBelowMinimum_Throws()
        {
            var policy = new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "00:30:00" // 30 minutes, below 1 day minimum
            };

            var ex = Assert.Throws<ArgumentException>(() => policy.Validate());
            Assert.Contains("at least 1 day", ex.Message);
        }

        [Fact]
        public void QueryAccelerationPolicy_Validate_HotMissing_Throws()
        {
            var policy = new QueryAccelerationPolicy
            {
                IsEnabled = true,
            };

            var ex = Assert.Throws<ArgumentException>(() => policy.Validate());
            Assert.Contains("Hot period is required", ex.Message);
        }

        [Fact]
        public void QueryAccelerationPolicy_Validate_InvalidHotFormat_Throws()
        {
            var policy = new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "not-a-timespan"
            };

            var ex = Assert.Throws<ArgumentException>(() => policy.Validate());
            Assert.Contains("Invalid Hot period format", ex.Message);
        }

        [Fact]
        public void QueryAccelerationPolicy_Validate_ExactlyOneDay_Succeeds()
        {
            var policy = new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "1.00:00:00"
            };

            // Should not throw
            policy.Validate();
        }

        #endregion

        #region YAML Serialization

        [Fact]
        public void ExternalTable_YamlRoundTrip_PreservesQueryAcceleration()
        {
            var table = CreateDeltaTable(new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "7.00:00:00",
                MaxAge = "00:05:00",
                ManagedIdentity = "12345678-1234-1234-1234-1234567890ab",
                HotDateTimeColumn = "EventTimestamp",
                HotWindows = new List<HotWindow>
                {
                    new HotWindow { MinValue = "2025-01-01 00:00:00", MaxValue = "2025-01-31 23:59:59" }
                }
            });

            var yaml = KustoSchemaTools.Helpers.Serialization.YamlPascalCaseSerializer.Serialize(table);
            var deserialized = KustoSchemaTools.Helpers.Serialization.YamlPascalCaseDeserializer.Deserialize<ExternalTable>(yaml);

            Assert.NotNull(deserialized.QueryAcceleration);
            Assert.True(deserialized.QueryAcceleration!.IsEnabled);
            Assert.Equal("7.00:00:00", deserialized.QueryAcceleration.Hot);
            Assert.Equal("00:05:00", deserialized.QueryAcceleration.MaxAge);
            Assert.Equal("12345678-1234-1234-1234-1234567890ab", deserialized.QueryAcceleration.ManagedIdentity);
            Assert.Equal("EventTimestamp", deserialized.QueryAcceleration.HotDateTimeColumn);
            Assert.NotNull(deserialized.QueryAcceleration.HotWindows);
            Assert.Single(deserialized.QueryAcceleration.HotWindows!);
            Assert.Equal("2025-01-01 00:00:00", deserialized.QueryAcceleration.HotWindows![0].MinValue);
            Assert.Equal("2025-01-31 23:59:59", deserialized.QueryAcceleration.HotWindows![0].MaxValue);
        }

        [Fact]
        public void ExternalTable_YamlRoundTrip_NullQueryAcceleration_PreservesNull()
        {
            var table = CreateDeltaTable();

            var yaml = KustoSchemaTools.Helpers.Serialization.YamlPascalCaseSerializer.Serialize(table);
            var deserialized = KustoSchemaTools.Helpers.Serialization.YamlPascalCaseDeserializer.Deserialize<ExternalTable>(yaml);

            Assert.Null(deserialized.QueryAcceleration);
        }

        [Fact]
        public void ExternalTable_YamlSerialization_UsesCamelCasePropertyNames()
        {
            var table = CreateDeltaTable(new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "7.00:00:00",
                HotDateTimeColumn = "EventTimestamp"
            });

            var yaml = KustoSchemaTools.Helpers.Serialization.YamlPascalCaseSerializer.Serialize(table);

            Assert.Contains("queryAcceleration:", yaml);
            Assert.Contains("isEnabled:", yaml);
            Assert.Contains("hot:", yaml);
            Assert.Contains("hotDateTimeColumn:", yaml);
        }

        #endregion

        #region JSON Serialization (Kusto command format)

        [Fact]
        public void QueryAccelerationPolicy_JsonSerialization_UsesPascalCasePropertyNames()
        {
            var policy = new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "7.00:00:00",
                HotDateTimeColumn = "EventTimestamp"
            };

            var json = JsonConvert.SerializeObject(policy, new JsonSerializerSettings
            {
                ContractResolver = new KustoSchemaTools.Helpers.Serialization.PascalCaseContractResolver(),
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });

            var parsed = JObject.Parse(json);
            Assert.NotNull(parsed["IsEnabled"]);
            Assert.NotNull(parsed["Hot"]);
            Assert.NotNull(parsed["HotDateTimeColumn"]);
            // Should NOT have lowercase keys
            Assert.Null(parsed["isEnabled"]);
            Assert.Null(parsed["hot"]);
        }

        [Fact]
        public void QueryAccelerationPolicy_MultipleHotWindows_SerializesCorrectly()
        {
            var table = CreateDeltaTable(new QueryAccelerationPolicy
            {
                IsEnabled = true,
                Hot = "7.00:00:00",
                HotWindows = new List<HotWindow>
                {
                    new HotWindow { MinValue = "2025-01-01 00:00:00", MaxValue = "2025-01-15 00:00:00" },
                    new HotWindow { MinValue = "2025-06-01 00:00:00", MaxValue = "2025-06-15 00:00:00" }
                }
            });

            var scripts = table.CreateScripts("MyTable", true);
            var qaScript = scripts.Single(s => s.Kind == "QueryAccelerationPolicy");

            var text = qaScript.Script.Text;
            var jsonStart = text.IndexOf("'") + 1;
            var jsonEnd = text.LastIndexOf("'");
            var json = text.Substring(jsonStart, jsonEnd - jsonStart);
            var parsed = JObject.Parse(json);

            var hotWindows = parsed["HotWindows"] as JArray;
            Assert.NotNull(hotWindows);
            Assert.Equal(2, hotWindows!.Count);
        }

        #endregion
    }
}
