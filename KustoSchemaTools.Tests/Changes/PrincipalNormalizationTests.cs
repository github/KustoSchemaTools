using KustoSchemaTools.Model;
using KustoSchemaTools.Changes;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests.Changes
{
    public class PrincipalNormalizationTests
    {
        private const string ObjectId = "8749feae-888c-446b-9f38-26f0c38ba1cd";
        private const string ClientId = "1de2a36c-bba4-4380-be8d-5f400303219b";
        private const string TenantId = "398a6654-997b-47e9-b12b-9515b896b4de";

        [Fact]
        public void NormalizePrincipalIds_ReplacesClientIdWithObjectId()
        {
            var db = new Database
            {
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new() { ObjectId = ObjectId, ClientId = ClientId }
                    }
                },
                Admins = new List<AADObject>
                {
                    new() { Name = "my-identity", Id = $"aadapp={ClientId};{TenantId}" }
                }
            };

            db.NormalizePrincipalIds();

            Assert.Equal($"aadapp={ObjectId};{TenantId}", db.Admins[0].Id);
        }

        [Fact]
        public void NormalizePrincipalIds_DoesNotChangeUnrelatedPrincipals()
        {
            var unrelatedId = "aadapp=99999999-9999-9999-9999-999999999999;tenant";
            var db = new Database
            {
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new() { ObjectId = ObjectId, ClientId = ClientId }
                    }
                },
                Admins = new List<AADObject>
                {
                    new() { Name = "other-app", Id = unrelatedId }
                }
            };

            db.NormalizePrincipalIds();

            Assert.Equal(unrelatedId, db.Admins[0].Id);
        }

        [Fact]
        public void NormalizePrincipalIds_NormalizesAllRoleLists()
        {
            var db = new Database
            {
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new() { ObjectId = ObjectId, ClientId = ClientId }
                    }
                },
                Admins = new List<AADObject> { new() { Name = "a", Id = $"aadapp={ClientId};t" } },
                Users = new List<AADObject> { new() { Name = "u", Id = $"aadapp={ClientId};t" } },
                Viewers = new List<AADObject> { new() { Name = "v", Id = $"aadapp={ClientId};t" } },
                UnrestrictedViewers = new List<AADObject> { new() { Name = "uv", Id = $"aadapp={ClientId};t" } },
                Ingestors = new List<AADObject> { new() { Name = "i", Id = $"aadapp={ClientId};t" } },
                Monitors = new List<AADObject> { new() { Name = "m", Id = $"aadapp={ClientId};t" } },
            };

            db.NormalizePrincipalIds();

            Assert.All(new[] { db.Admins, db.Users, db.Viewers, db.UnrestrictedViewers, db.Ingestors, db.Monitors },
                list => Assert.Contains(ObjectId, list[0].Id));
        }

        [Fact]
        public void NormalizePrincipalIds_NoManagedIdentity_NoOp()
        {
            var originalId = $"aadapp={ClientId};tenant";
            var db = new Database
            {
                Admins = new List<AADObject>
                {
                    new() { Name = "app", Id = originalId }
                }
            };

            db.NormalizePrincipalIds();

            Assert.Equal(originalId, db.Admins[0].Id);
        }

        [Fact]
        public void NormalizePrincipalIds_NullClientId_SkipsPolicy()
        {
            var originalId = $"aadapp={ClientId};tenant";
            var db = new Database
            {
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new() { ObjectId = ObjectId, ClientId = null }
                    }
                },
                Admins = new List<AADObject>
                {
                    new() { Name = "app", Id = originalId }
                }
            };

            db.NormalizePrincipalIds();

            Assert.Equal(originalId, db.Admins[0].Id);
        }

        [Fact]
        public void NormalizePrincipalIds_SameClientAndObjectId_SkipsPolicy()
        {
            var originalId = $"aadapp={ObjectId};tenant";
            var db = new Database
            {
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new() { ObjectId = ObjectId, ClientId = ObjectId }
                    }
                },
                Admins = new List<AADObject>
                {
                    new() { Name = "app", Id = originalId }
                }
            };

            db.NormalizePrincipalIds();

            Assert.Equal(originalId, db.Admins[0].Id);
        }

        [Fact]
        public void PermissionChange_NoPhantomDiff_AfterNormalization()
        {
            // Simulate: YAML has ObjectId, cluster loaded with ClientId then normalized
            var yamlAdmins = new List<AADObject>
            {
                new() { Name = "regular-app", Id = $"aadapp=aaaa;{TenantId}" },
                new() { Name = "my-identity", Id = $"aadapp={ObjectId};{TenantId}" },
            };

            // Cluster originally returned ClientId, but after normalization it has ObjectId
            var clusterAdmins = new List<AADObject>
            {
                new() { Name = "regular-app", Id = $"aadapp=aaaa;{TenantId}" },
                new() { Name = "my-identity", Id = $"aadapp={ObjectId};{TenantId}" },
            };

            var change = new PermissionChange("testdb", "Admins", clusterAdmins, yamlAdmins);

            // No scripts should be generated since the lists are identical after normalization
            Assert.Empty(change.Scripts);
        }

        [Fact]
        public void PermissionChange_DetectsRealDiff_WithNormalization()
        {
            // YAML adds a new admin that wasn't in the cluster
            var yamlAdmins = new List<AADObject>
            {
                new() { Name = "existing-app", Id = $"aadapp=aaaa;{TenantId}" },
                new() { Name = "new-app", Id = $"aadapp=bbbb;{TenantId}" },
            };

            var clusterAdmins = new List<AADObject>
            {
                new() { Name = "existing-app", Id = $"aadapp=aaaa;{TenantId}" },
            };

            var change = new PermissionChange("testdb", "Admins", clusterAdmins, yamlAdmins);

            // Should generate a script because there's a real difference
            Assert.NotEmpty(change.Scripts);
        }

        [Fact]
        public void PermissionChange_PhantomDiff_WithoutNormalization()
        {
            // Without normalization: YAML has ObjectId, cluster has ClientId → phantom diff
            var yamlAdmins = new List<AADObject>
            {
                new() { Name = "my-identity", Id = $"aadapp={ObjectId};{TenantId}" },
            };

            var clusterAdmins = new List<AADObject>
            {
                new() { Name = "my-identity", Id = $"aadapp={ClientId};{TenantId}" },
            };

            var change = new PermissionChange("testdb", "Admins", clusterAdmins, yamlAdmins);

            // Without normalization, this would incorrectly generate a script
            Assert.NotEmpty(change.Scripts);
        }

        [Fact]
        public void NormalizePrincipalIds_MultipleManagedIdentities_AllNormalized()
        {
            var objectId2 = "aaaa-bbbb-cccc";
            var clientId2 = "dddd-eeee-ffff";

            var db = new Database
            {
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new() { ObjectId = ObjectId, ClientId = ClientId },
                        new() { ObjectId = objectId2, ClientId = clientId2 },
                    }
                },
                Admins = new List<AADObject>
                {
                    new() { Name = "id1", Id = $"aadapp={ClientId};t" },
                    new() { Name = "id2", Id = $"aadapp={clientId2};t" },
                }
            };

            db.NormalizePrincipalIds();

            Assert.Equal($"aadapp={ObjectId};t", db.Admins[0].Id);
            Assert.Equal($"aadapp={objectId2};t", db.Admins[1].Id);
        }

        [Fact]
        public void ParseFqn_StandardFormat()
        {
            var (kind, guid, rest) = Database.ParseFqn("aadapp=8749feae-888c-446b-9f38-26f0c38ba1cd;398a6654-997b-47e9-b12b-9515b896b4de");

            Assert.Equal("aadapp", kind);
            Assert.Equal("8749feae-888c-446b-9f38-26f0c38ba1cd", guid);
            Assert.Equal(";398a6654-997b-47e9-b12b-9515b896b4de", rest);
        }

        [Fact]
        public void ParseFqn_NoSemicolon()
        {
            var (kind, guid, rest) = Database.ParseFqn("aadapp=someguid");

            Assert.Equal("aadapp", kind);
            Assert.Equal("someguid", guid);
            Assert.Equal("", rest);
        }

        [Fact]
        public void ParseFqn_NoEquals_ReturnsNulls()
        {
            var (kind, guid, rest) = Database.ParseFqn("invalidformat");

            Assert.Null(kind);
            Assert.Null(guid);
        }

        [Fact]
        public void NormalizePrincipalIds_OnlyNormalizesAadappKind()
        {
            var db = new Database
            {
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new() { ObjectId = ObjectId, ClientId = ClientId }
                    }
                },
                Admins = new List<AADObject>
                {
                    new() { Name = "group", Id = $"aadgroup={ClientId};{TenantId}" }
                }
            };

            db.NormalizePrincipalIds();

            // aadgroup should NOT be normalized, only aadapp
            Assert.Equal($"aadgroup={ClientId};{TenantId}", db.Admins[0].Id);
        }

        [Fact]
        public void NormalizePrincipalIds_CaseInsensitiveClientIdMatch()
        {
            var upperClientId = ClientId.ToUpperInvariant();
            var db = new Database
            {
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new() { ObjectId = ObjectId, ClientId = ClientId }
                    }
                },
                Admins = new List<AADObject>
                {
                    new() { Name = "my-identity", Id = $"aadapp={upperClientId};{TenantId}" }
                }
            };

            db.NormalizePrincipalIds();

            Assert.Equal($"aadapp={ObjectId};{TenantId}", db.Admins[0].Id);
        }

        [Fact]
        public void NormalizePrincipalIds_DuplicateClientIds_HandledGracefully()
        {
            var db = new Database
            {
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new() { ObjectId = ObjectId, ClientId = ClientId },
                        new() { ObjectId = ObjectId, ClientId = ClientId }, // duplicate
                    }
                },
                Admins = new List<AADObject>
                {
                    new() { Name = "my-identity", Id = $"aadapp={ClientId};{TenantId}" }
                }
            };

            // Should not throw
            db.NormalizePrincipalIds();

            Assert.Equal($"aadapp={ObjectId};{TenantId}", db.Admins[0].Id);
        }
    }
}
