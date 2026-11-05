using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests.Changes
{
    public class FollowerPermissionChangeTests
    {
        private readonly Mock<ILogger> _logger = new();

        private static FollowerDatabase BuildFollower(params (string role, string id, string name)[] principals)
        {
            var follower = new FollowerDatabase
            {
                DatabaseName = "DDoSNeuralAnalysis",
                Permissions = new FollowerPermissions { ModificationKind = FollowerModificationKind.Union }
            };

            foreach (var (role, id, name) in principals)
            {
                var obj = new AADObject { Id = id, Name = name };
                if (string.Equals(role, "admin", StringComparison.OrdinalIgnoreCase))
                {
                    follower.Permissions.Admins.Add(obj);
                }
                else
                {
                    follower.Permissions.Viewers.Add(obj);
                }
            }

            return follower;
        }

        [Fact]
        public void GeneratesFollowerAdd_WithLeaderName()
        {
            var oldFollower = BuildFollower();
            var newFollower = BuildFollower(("viewer", "aadapp=64decea3-723a-4fbf-b2ec-9faaf852cfdc;398a6654-997b-47e9-b12b-9515b896b4de", "spn-dev-spam-slam"));
            newFollower.Permissions.LeaderName = "leader-cluster";

            var changes = DatabaseChanges.GenerateFollowerChanges(oldFollower, newFollower, _logger.Object);

            var permChange = Assert.Single(changes.OfType<FollowerPermissionChange>());
            var script = Assert.Single(permChange.Scripts).Script!.Text;

            Assert.Equal(".add follower database DDoSNeuralAnalysis viewers (\"aadapp=64decea3-723a-4fbf-b2ec-9faaf852cfdc;398a6654-997b-47e9-b12b-9515b896b4de\") 'leader-cluster'", script);
        }

        [Fact]
        public void GeneratesFollowerAdd_WithoutLeaderName()
        {
            var oldFollower = BuildFollower();
            var newFollower = BuildFollower(("admin", "aaduser=foo;tenant", "Foo"));

            var changes = DatabaseChanges.GenerateFollowerChanges(oldFollower, newFollower, _logger.Object);

            var permChange = Assert.Single(changes.OfType<FollowerPermissionChange>());
            var script = Assert.Single(permChange.Scripts).Script!.Text;

            Assert.Equal(".add follower database DDoSNeuralAnalysis admins (\"aaduser=foo;tenant\")", script);
        }

        [Fact]
        public void EmitsDrop_WhenRemovingPrincipals()
        {
            var oldFollower = BuildFollower(("admin", "aadapp=1;tenant", "v1"));
            var newFollower = BuildFollower();

            var changes = DatabaseChanges.GenerateFollowerChanges(oldFollower, newFollower, _logger.Object);

            var permChange = Assert.Single(changes.OfType<FollowerPermissionChange>());
            var script = Assert.Single(permChange.Scripts).Script!.Text;

            Assert.Equal(".drop follower database DDoSNeuralAnalysis admins (\"aadapp=1;tenant\")", script);
        }
    }
}
