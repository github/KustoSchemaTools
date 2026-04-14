using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class QueryAccelerationPolicy
    {
        public bool IsEnabled { get; set; }

        public string Hot { get; set; } // timespan, e.g. "7.00:00:00". Minimum 1 day.

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public List<HotWindow>? HotWindows { get; set; }

        public bool ShouldSerializeHotWindows() => HotWindows?.Count > 0;

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string? MaxAge { get; set; } // timespan, e.g. "00:05:00". Default 5 min, minimum 1 min.

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string? ManagedIdentity { get; set; } // GUID string, ADX only

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string? HotDateTimeColumn { get; set; }

        public void Validate()
        {
            if (!IsEnabled)
                return;

            if (string.IsNullOrWhiteSpace(Hot))
                throw new ArgumentException("Hot period is required when query acceleration is enabled");

            if (!TryParseKustoTimespan(Hot, out var hotPeriod))
                throw new ArgumentException($"Invalid Hot period format: {Hot}. Expected a timespan like '7d' or '7.00:00:00'");

            if (hotPeriod < TimeSpan.FromDays(1))
                throw new ArgumentException($"Hot period must be at least 1 day. Got: {Hot}");
        }

        /// <summary>
        /// Normalizes timespan fields to the format returned by the cluster (e.g. "7d" → "7.00:00:00")
        /// to avoid phantom diffs when comparing YAML against cluster state.
        /// </summary>
        public QueryAccelerationPolicy Normalize()
        {
            return new QueryAccelerationPolicy
            {
                IsEnabled = IsEnabled,
                Hot = Hot != null ? NormalizeTimespan(Hot) : null,
                HotWindows = HotWindows,
                MaxAge = MaxAge != null ? NormalizeTimespan(MaxAge) : null,
                ManagedIdentity = ManagedIdentity,
                HotDateTimeColumn = HotDateTimeColumn
            };
        }

        private static string NormalizeTimespan(string value)
        {
            return TryParseKustoTimespan(value, out var ts) ? ts.ToString() : value;
        }

        private static bool TryParseKustoTimespan(string value, out TimeSpan result)
        {
            if (TimeSpan.TryParse(value, out result))
                return true;

            // Support Kusto shorthand: e.g. "7d", "30d"
            if (value.EndsWith("d", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(value.AsSpan(0, value.Length - 1), out var days))
            {
                result = TimeSpan.FromDays(days);
                return true;
            }

            return false;
        }
    }

    public class HotWindow
    {
        public string MinValue { get; set; }
        public string MaxValue { get; set; }
    }
}
