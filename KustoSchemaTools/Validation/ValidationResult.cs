using KustoSchemaTools.Changes;

namespace KustoSchemaTools.Validation
{
    /// <summary>
    /// Represents the result of a validation operation.
    /// </summary>
    public class ValidationResult
    {
        private ValidationResult(bool isValid, string? errorMessage, CommentKind? severity)
        {
            IsValid = isValid;
            ErrorMessage = errorMessage;
            Severity = severity;
        }

        /// <summary>
        /// Gets a value indicating whether the validation passed.
        /// </summary>
        public bool IsValid { get; }

        /// <summary>
        /// Gets the error message if validation failed, otherwise null.
        /// </summary>
        public string? ErrorMessage { get; }

        /// <summary>
        /// Gets the severity level of the validation failure, if applicable.
        /// </summary>
        public CommentKind? Severity { get; }

        /// <summary>
        /// Creates a successful validation result.
        /// </summary>
        public static ValidationResult Success()
        {
            return new ValidationResult(true, null, null);
        }

        /// <summary>
        /// Creates a failed validation result with the specified error message and severity.
        /// </summary>
        public static ValidationResult Failure(string errorMessage, CommentKind severity)
        {
            return new ValidationResult(false, errorMessage, severity);
        }
    }
}
