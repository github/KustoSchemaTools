using KustoSchemaTools.Validation;
using System.Text;

namespace KustoSchemaTools.Exceptions
{
    /// <summary>
    /// Exception thrown when AAD validation fails for one or more principals
    /// </summary>
    public class AADValidationException : Exception
    {
        public List<AADValidationResult> ValidationResults { get; }
        public List<AADValidationResult> FailedValidations { get; }
        
        public AADValidationException(List<AADValidationResult> validationResults) 
            : base(CreateMessage(validationResults))
        {
            ValidationResults = validationResults;
            FailedValidations = validationResults.Where(r => !r.IsValid).ToList();
        }
        
        private static string CreateMessage(List<AADValidationResult> validationResults)
        {
            var failed = validationResults.Where(r => !r.IsValid).ToList();
            var valid = validationResults.Where(r => r.IsValid).ToList();
            
            var message = new StringBuilder();
            message.AppendLine($"AAD validation failed: {failed.Count} invalid, {valid.Count} valid principals");
            message.AppendLine();
            
            if (failed.Any())
            {
                message.AppendLine("FAILED validations:");
                foreach (var result in failed)
                {
                    message.AppendLine($"  ❌ {result.Id} ({result.Type}) - {result.ErrorMessage}");
                }
                message.AppendLine();
            }
            
            if (valid.Any())
            {
                message.AppendLine("SUCCESSFUL validations:");
                foreach (var result in valid)
                {
                    var name = !string.IsNullOrEmpty(result.Name) ? $" ({result.Name})" : "";
                    message.AppendLine($"  ✅ {result.Id} ({result.Type}){name}");
                }
            }
            
            return message.ToString();
        }
    }
}
