using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DiffPlex;
using DiffPlex.DiffBuilder;
using DiffPlex.DiffBuilder.Model;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Changes
{
    public static class StructuredChangeExtensions
    {
        public static StructuredChange ToStructuredChange(this IChange change)
        {
            ArgumentNullException.ThrowIfNull(change);

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
                    structuredChange.DiffMarkdown = BuildDiffMarkdown(scriptCompare);
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
                var previousScripts = BuildPreviousScripts(change.From, change.Entity);

                foreach (var script in comparison.NewScripts)
                {
                    if (previousScripts.TryGetValue(script.Kind, out var previous))
                    {
                        comparison.OldScripts.Add(CloneScript(previous));
                    }
                }

                var validationPayload = BuildValidationPayload(previousScripts, comparison.NewScripts);
                if (validationPayload.Count > 0)
                {
                    comparison.ValidationResults = validationPayload;
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

            if (source.Diagnostics != null && source.Diagnostics.Count > 0)
            {
                clone.Diagnostics = source.Diagnostics
                    .Select(diagnostic => new ScriptDiagnostic
                    {
                        Start = diagnostic.Start,
                        End = diagnostic.End,
                        Description = diagnostic.Description
                    })
                    .ToList();
            }

            return clone;
        }

        private static Dictionary<string, object?> BuildValidationPayload(Dictionary<string, DatabaseScriptContainer> previousScripts, List<DatabaseScriptContainer> newScripts)
        {
            var payload = new Dictionary<string, object?>();
            foreach (var script in newScripts)
            {
                previousScripts.TryGetValue(script.Kind, out var oldScript);
                var diffPreview = BuildDiffPreview(oldScript, script);
                if (diffPreview.Count > 0)
                {
                    var keyName = string.IsNullOrWhiteSpace(script.Kind) ? "diff" : $"diff::{script.Kind}";
                    payload[keyName] = diffPreview;
                }
            }

            return payload;
        }

        private static List<string> BuildDiffPreview(DatabaseScriptContainer? oldScript, DatabaseScriptContainer? newScript)
        {
            var before = GetScriptText(oldScript);
            var after = GetScriptText(newScript);

            if (string.Equals(before, after, StringComparison.Ordinal))
            {
                return new List<string>();
            }

            var differ = new Differ();
            var diff = InlineDiffBuilder.Diff(before, after, false);

            var preview = diff.Lines
                .Where(line => line.Type != ChangeType.Unchanged)
                .Select(line =>
                {
                    var prefix = line.Type switch
                    {
                        ChangeType.Inserted => "+",
                        ChangeType.Deleted => "-",
                        ChangeType.Modified => "~",
                        _ => " "
                    };
                    return $"{prefix}{line.Text?.TrimEnd()}";
                })
                .Where(line => !string.IsNullOrWhiteSpace(line))
                .Take(10)
                .ToList();

            return preview;
        }

        private static string GetScriptText(DatabaseScriptContainer? script)
        {
            return script?.Script?.Text ?? string.Empty;
        }

        private static string? BuildDiffMarkdown(ScriptCompareChange change)
        {
            var previousScripts = BuildPreviousScripts(change.From, change.Entity);

            var sb = new StringBuilder();
            var differ = new Differ();
            foreach (var script in change.Scripts)
            {
                var before = previousScripts.TryGetValue(script.Kind, out var prior)
                    ? prior.Script?.Text ?? string.Empty
                    : string.Empty;
                var after = script.Script?.Text ?? string.Empty;

                if (string.Equals(before, after, StringComparison.Ordinal))
                {
                    continue;
                }

                var diff = InlineDiffBuilder.Diff(before, after, false);
                var hasMeaningfulDiff = diff.Lines.Any(line => line.Type != ChangeType.Unchanged);
                if (!hasMeaningfulDiff)
                {
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(script.Kind))
                {
                    sb.AppendLine($"// {script.Kind}");
                }

                sb.AppendLine("```diff");
                foreach (var line in diff.Lines)
                {
                    var prefix = line.Type switch
                    {
                        ChangeType.Inserted => "+",
                        ChangeType.Deleted => "-",
                        ChangeType.Modified => "~",
                        _ => " "
                    };
                    sb.AppendLine($"{prefix}{line.Text}");
                }
                sb.AppendLine("```");
                sb.AppendLine();
            }

            var diffContent = sb.ToString().Trim();
            return string.IsNullOrEmpty(diffContent) ? null : diffContent;
        }

        private static Dictionary<string, DatabaseScriptContainer> BuildPreviousScripts(IKustoBaseEntity? entity, string changeEntity)
        {
            if (entity == null)
            {
                return new Dictionary<string, DatabaseScriptContainer>();
            }

            return entity
                .CreateScripts(changeEntity, false)
                .GroupBy(script => script.Kind)
                .Select(group => group.First())
                .ToDictionary(script => script.Kind, script => script);
        }
    }
}
