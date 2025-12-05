using System.Collections.Generic;
using KustoSchemaTools.Changes;
using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class StructuredDiffResult
    {
        [JsonProperty("isValid")]
        public bool IsValid { get; set; }

        [JsonProperty("diffs")]
        public List<StructuredDiff> Diffs { get; set; } = new List<StructuredDiff>();

        [JsonProperty("message", NullValueHandling = NullValueHandling.Ignore)]
        public string? Message { get; set; }
    }

    public class StructuredDiff
    {
        [JsonProperty("clusterName")]
        public string ClusterName { get; set; } = string.Empty;

        [JsonProperty("clusterUrl")]
        public string ClusterUrl { get; set; } = string.Empty;

        [JsonProperty("databaseName")]
        public string DatabaseName { get; set; } = string.Empty;

        [JsonProperty("isValid")]
        public bool IsValid { get; set; }

        [JsonProperty("comments")]
        public List<StructuredComment> Comments { get; set; } = new List<StructuredComment>();

        [JsonProperty("changes")]
        public List<StructuredChange> Changes { get; set; } = new List<StructuredChange>();

        [JsonProperty("validScripts")]
        public List<DatabaseScriptContainer> ValidScripts { get; set; } = new List<DatabaseScriptContainer>();
    }

    public class StructuredChange
    {
        [JsonProperty("entityType")]
        public string EntityType { get; set; } = string.Empty;

        [JsonProperty("entity")]
        public string Entity { get; set; } = string.Empty;

        [JsonProperty("changeType")]
        public string ChangeType { get; set; } = string.Empty;

        [JsonProperty("scripts")]
        public List<DatabaseScriptContainer> Scripts { get; set; } = new List<DatabaseScriptContainer>();

        [JsonProperty("comment", NullValueHandling = NullValueHandling.Ignore)]
        public StructuredComment? Comment { get; set; }

        [JsonProperty("scriptComparison", NullValueHandling = NullValueHandling.Ignore)]
        public StructuredScriptComparison? ScriptComparison { get; set; }

        [JsonProperty("deletedEntities")]
        public List<string> DeletedEntities { get; set; } = new List<string>();

        [JsonProperty("headingText", NullValueHandling = NullValueHandling.Ignore)]
        public string? HeadingText { get; set; }

        [JsonProperty("diffMarkdown", NullValueHandling = NullValueHandling.Ignore)]
        public string? DiffMarkdown { get; set; }
    }

    public class StructuredScriptComparison
    {
        [JsonProperty("oldScripts")]
        public List<DatabaseScriptContainer> OldScripts { get; set; } = new List<DatabaseScriptContainer>();

        [JsonProperty("newScripts")]
        public List<DatabaseScriptContainer> NewScripts { get; set; } = new List<DatabaseScriptContainer>();

        [JsonProperty("validationResults", NullValueHandling = NullValueHandling.Ignore)]
        public Dictionary<string, object?>? ValidationResults { get; set; }
    }

    public class StructuredComment
    {
        [JsonProperty("kind")]
        public string Kind { get; set; } = string.Empty;

        [JsonProperty("text")]
        public string Text { get; set; } = string.Empty;

        [JsonProperty("failsRollout")]
        public bool FailsRollout { get; set; }

        public static StructuredComment? From(Comment? source)
        {
            if (source == null)
            {
                return null;
            }

            return new StructuredComment
            {
                Kind = source.Kind.ToString(),
                Text = source.Text,
                FailsRollout = source.FailsRollout
            };
        }
    }
}
