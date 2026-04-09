using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class QueryAccelerationPolicy
    {
        public bool IsEnabled { get; set; }

        public string Hot { get; set; } // timespan, e.g. "7.00:00:00". Minimum 1 day.

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public List<HotWindow>? HotWindows { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string? MaxAge { get; set; } // timespan, e.g. "00:05:00". Default 5 min, minimum 1 min.

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string? ManagedIdentity { get; set; } // GUID string, ADX only

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string? HotDateTimeColumn { get; set; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Hot))
                throw new ArgumentException("Hot period is required for query acceleration policy");

            if (!TimeSpan.TryParse(Hot, out var hotPeriod))
                throw new ArgumentException($"Invalid Hot period format: {Hot}. Expected a timespan like '7.00:00:00'");

            if (hotPeriod < TimeSpan.FromDays(1))
                throw new ArgumentException($"Hot period must be at least 1 day (1.00:00:00). Got: {Hot}");
        }
    }

    public class HotWindow
    {
        public string MinValue { get; set; }
        public string MaxValue { get; set; }
    }
}
