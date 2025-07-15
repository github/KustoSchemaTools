using KustoSchemaTools.Model;

namespace KustoSchemaTools.Validation
{
    /// <summary>
    /// Interface for validating AAD objects against Azure Active Directory
    /// </summary>
    public interface IAADObjectValidator
    {
        /// <summary>
        /// Validates a single AAD object against the configured tenant
        /// </summary>
        /// <param name="aadObject">The AAD object to validate</param>
        /// <returns>Validation result with details</returns>
        Task<AADValidationResult> ValidateAADObjectAsync(AADObject aadObject);

        /// <summary>
        /// Validates a collection of AAD objects against the configured tenant
        /// </summary>
        /// <param name="aadObjects">The AAD objects to validate</param>
        /// <returns>Collection of validation results</returns>
        Task<IEnumerable<AADValidationResult>> ValidateAADObjectsAsync(IEnumerable<AADObject> aadObjects);

        /// <summary>
        /// Validates an AAD object by ID and type
        /// </summary>
        /// <param name="id">The AAD object ID (GUID or UPN)</param>
        /// <param name="type">The type of AAD object (user, group, application)</param>
        /// <returns>Validation result with details</returns>
        Task<AADValidationResult> ValidateAADObjectByIdAsync(string id, AADObjectType type);
    }

    /// <summary>
    /// Result of AAD object validation
    /// </summary>
    public class AADValidationResult
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public AADObjectType Type { get; set; }
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public bool Exists { get; set; }
        public DateTime? ValidatedAt { get; set; }
    }

    /// <summary>
    /// Types of AAD objects that can be validated
    /// </summary>
    public enum AADObjectType
    {
        Unknown,
        User,
        Group,
        Application,
        ServicePrincipal
    }
}
