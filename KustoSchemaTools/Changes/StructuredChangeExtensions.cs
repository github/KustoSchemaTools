using System;
using System.Collections.Generic;
using System.Linq;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Changes
{
    public static class StructuredChangeExtensions
    {
        public static StructuredChange ToStructuredChange(this IChange change)
        {
            if (change == null)
            {
                throw new ArgumentNullException(nameof(change));
            }

            var structuredChange = new StructuredChange
            {
                EntityType = change.EntityType,
                Entity = change.Entity,
                Scripts = change.Scripts?.Select(CloneScript).ToList() ?? new List<DatabaseScriptContainer>(),
                Comment = StructuredComment.From(change.Comment)
            };

            switch (change)
            {
                case Heading heading:
                    structuredChange.ChangeType = "Heading";
                    structuredChange.HeadingText = heading.Entity;
                    structuredChange.Scripts.Clear();
                    break;
                case DeletionChange deletion:
                    structuredChange.ChangeType = "Delete";
                    structuredChange.DeletedEntities = new List<string> { deletion.Entity };
                    break;
                case ScriptCompareChange scriptCompare:
                    structuredChange.ChangeType = scriptCompare.From == null ? "Create" : "Update";
                    structuredChange.ScriptComparison = scriptCompare.ToStructuredScriptComparison();
                    break;
                default:
                    structuredChange.ChangeType = "Update";
                    break;
            }

            structuredChange.DeletedEntities ??= new List<string>();

            return structuredChange;
        }

        private static StructuredScriptComparison? ToStructuredScriptComparison(this ScriptCompareChange change)
        {
            var comparison = new StructuredScriptComparison
            {
                NewScripts = change.Scripts?.Select(CloneScript).ToList() ?? new List<DatabaseScriptContainer>()
            };

            if (change.From != null)
            {
                var previousScripts = change.From
                    .CreateScripts(change.Entity, false)
                    .GroupBy(script => script.Kind)
                    .Select(group => group.First())
                    .ToDictionary(script => script.Kind, script => script);

                foreach (var script in comparison.NewScripts)
                {
                    if (previousScripts.TryGetValue(script.Kind, out var previous))
                    {
                        comparison.OldScripts.Add(CloneScript(previous));
                    }
                }
            }

            foreach (var script in comparison.OldScripts.Where(s => !s.IsValid.HasValue))
            {
                script.IsValid = true;
            }

            return comparison;
        }

        private static DatabaseScriptContainer CloneScript(DatabaseScriptContainer source)
        {
            var clone = new DatabaseScriptContainer(new DatabaseScript(source.Script.Text, source.Script.Order), source.Kind, source.IsAsync)
            {
                IsValid = source.IsValid
            };

            return clone;
        }
    }
}
