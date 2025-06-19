using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class Policy
    {
        public string? Retention { get; set; }
        public string? HotCache { get; set; }
        public PartitioningPolicy? Partitioning { get; set; }
        public string? RowLevelSecurity { get; set; }


        public List<DatabaseScriptContainer> CreateScripts(string name, string entity)
        {
            var scripts = new List<DatabaseScriptContainer>();
            if (Retention != null)
            {
                scripts.Add(new DatabaseScriptContainer("SoftDelete", 60, $".alter-merge {entity} {name} policy retention softdelete={Retention}"));
            }
            if (HotCache != null)
            {
                scripts.Add(new DatabaseScriptContainer("HotCache", 70, $".alter {entity} {name} policy caching hot={HotCache}"));
            }
          
            if (!string.IsNullOrEmpty(RowLevelSecurity))
            {
                scripts.Add(new DatabaseScriptContainer("RowLevelSecurity", 57, $".alter {entity} {name} policy row_level_security enable ```{RowLevelSecurity}```"));
            }
            else
            {
                scripts.Add(new DatabaseScriptContainer("RowLevelSecurity", 52, $".delete {entity} {name} policy row_level_security"));
            }

            if (Partitioning != null)
            {
                scripts.Add(Partitioning.CreateScript(name, entity));
            }
            return scripts;
        }
    }

    public class TablePolicy : Policy
    {

        public List<UpdatePolicy>? UpdatePolicies { get; set; }
        public bool RestrictedViewAccess { get; set; } = false;
        
        /// <summary>
        /// Validates all update policies in this table policy against the target table and database context.
        /// </summary>
        /// <param name="targetTable">The table this policy will be applied to</param>
        /// <param name="database">The database context containing all tables</param>
        /// <returns>A collection of validation results for each update policy</returns>
        public List<UpdatePolicyValidationResult> ValidateUpdatePolicies(Table targetTable, Database database)
        {
            var results = new List<UpdatePolicyValidationResult>();
            
            if (UpdatePolicies == null || !UpdatePolicies.Any())
            {
                return results;
            }

            foreach (var updatePolicy in UpdatePolicies)
            {
                Table? sourceTable = null;
                if (database?.Tables?.ContainsKey(updatePolicy.Source) == true)
                {
                    sourceTable = database.Tables[updatePolicy.Source];
                }

                var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database ?? new Database());
                results.Add(result);
            }

            return results;
        }

        /// <summary>
        /// Creates scripts for this table policy, with optional validation.
        /// </summary>
        /// <param name="name">The table name</param>
        /// <param name="targetTable">The target table (optional, for validation)</param>
        /// <param name="database">The database context (optional, for validation)</param>
        /// <param name="validatePolicies">Whether to validate update policies before creating scripts</param>
        /// <returns>List of database script containers</returns>
        public List<DatabaseScriptContainer> CreateScripts(string name, Table? targetTable = null, Database? database = null, bool validatePolicies = false)
        {
            var scripts = new List<DatabaseScriptContainer>();
            scripts.AddRange(base.CreateScripts(name, "table"));

            if (UpdatePolicies != null)
            {
                // Validate update policies if requested and we have the necessary context
                if (validatePolicies && targetTable != null && database != null)
                {
                    var validationResults = ValidateUpdatePolicies(targetTable, database);
                    var hasErrors = validationResults.Any(r => !r.IsValid);
                    
                    if (hasErrors)
                    {
                        var errors = validationResults
                            .Where(r => !r.IsValid)
                            .SelectMany(r => r.Errors)
                            .ToList();
                        
                        throw new InvalidOperationException(
                            $"Update policy validation failed for table '{name}': {string.Join("; ", errors)}");
                    }
                    
                    // Log warnings if any
                    var warnings = validationResults
                        .Where(r => r.HasWarnings)
                        .SelectMany(r => r.Warnings)
                        .ToList();
                    
                    if (warnings.Any())
                    {
                        // In a production environment, you'd use a proper logging framework
                        Console.WriteLine($"Update policy warnings for table '{name}': {string.Join("; ", warnings)}");
                    }
                }

                var policies = JsonConvert.SerializeObject(UpdatePolicies, Serialization.JsonPascalCase);
                var upPriority = UpdatePolicies.Any() ? 59 : 50;
                scripts.Add(new DatabaseScriptContainer("TableUpdatePolicy", upPriority, $".alter table {name} policy update ```{policies}```"));
            }

            var rvaPrio = RestrictedViewAccess ? 58 : 51;
            scripts.Add(new DatabaseScriptContainer("RestrictedViewAccess", rvaPrio, $".alter table {name} policy restricted_view_access {(RestrictedViewAccess ? "true" : "false")}"));

            return scripts;
        }

        /// <summary>
        /// Creates scripts for this table policy (backward compatibility).
        /// </summary>
        /// <param name="name">The table name</param>
        /// <returns>List of database script containers</returns>
        public List<DatabaseScriptContainer> CreateScripts(string name)
        {
            return CreateScripts(name, null, null, false);
        }
    }

}
