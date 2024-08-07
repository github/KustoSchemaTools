﻿using KustoSchemaTools.Changes;
using System.ComponentModel;
using YamlDotNet.Core;
using YamlDotNet.Serialization;

namespace KustoSchemaTools.Model
{
    public class MaterializedView : IKustoBaseEntity
    {
        public string Source { get; set; }
        public string Kind { get; set; } = "table";
        public string Folder { get; set; }
        public string DocString { get; set; }
        public string? EffectiveDateTime { get; set; }
        public string Lookback { get; set; }
        public bool? UpdateExtentsCreationTime { get; set; }
        public bool? Backfill { get; set; }
        public bool AutoUpdateSchema { get; set; } = false;
        public List<string> DimensionTables { get; set; }
        public RetentionAndCachePolicy RetentionAndCachePolicy { get; set; } = new RetentionAndCachePolicy();
        [YamlMember(ScalarStyle = ScalarStyle.Literal)]
        public string Query { get; set; }
        public string? RowLevelSecurity { get; set; }

        public List<DatabaseScriptContainer> CreateScripts(string name, bool isNew)
        {
            var asyncSetup = isNew && Backfill == true && !string.IsNullOrWhiteSpace(EffectiveDateTime);


            var excludedProperies = new HashSet<string>(["Query", "Source", "Kind", "RetentionAndCachePolicy", "RowLevelSecurity"]);
            if (!asyncSetup)
            {
                excludedProperies.Add("EffectiveDateTime");
                excludedProperies.Add("Backfill");
            }

            var scripts = new List<DatabaseScriptContainer>();
            var properties = string.Join(", ", GetType().GetProperties()
                .Where(p => p.GetValue(this) != null && excludedProperies.Contains(p.Name) == false)
                .Select(p => new {Name = p.Name, Value = p.GetValue(this) })
                .Where(p => !string.IsNullOrWhiteSpace(p.Value?.ToString()))
                .Select(p => $"{p.Name}=\"{p.Value}\""));

            if (asyncSetup)
            {
                scripts.Add(new DatabaseScriptContainer("CreateMaterializedView", Kind == "table" ? 40 : 41, $".create async ifnotexists materialized-view with ({properties}) {name} on {Kind} {Source} {{ {Query} }}", true));
            }
            else
            {
                scripts.Add(new DatabaseScriptContainer("CreateMaterializedView", Kind == "table" ? 40 : 41, $".create-or-alter materialized-view with ({properties}) {name} on {Kind} {Source} {{ {Query} }}"));
            }

            if (RetentionAndCachePolicy != null)
            {
                scripts.AddRange(RetentionAndCachePolicy.CreateScripts(name, "materialized-view"));
            }


            if (!string.IsNullOrEmpty(RowLevelSecurity))
            {
                scripts.Add(new DatabaseScriptContainer("RowLevelSecurity", 57, $".alter materialized-view {name} policy row_level_security enable \"{RowLevelSecurity}\""));
            }
            else
            {
                scripts.Add(new DatabaseScriptContainer("RowLevelSecurity", 52, $".delete materialized-view {name} policy row_level_security"));
            }
            return scripts;
        }
    }

}
