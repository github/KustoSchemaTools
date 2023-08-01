using Azure.Core;
using Azure.Identity;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using KustoSchemaTools.Parser;

namespace FlowLoop.Shared.Kusto
{
    public class KustoClient
    {
        public KustoClient(string clusterName)
        {

            var azureServiceTokenProvider = new DefaultAzureCredential();
            var url = clusterName.ToKustoClusterUrl();
            var kustoConnectionStringBuilder = new KustoConnectionStringBuilder(url)
                .WithAadTokenProviderAuthentication(() =>
                    azureServiceTokenProvider.GetTokenAsync(new TokenRequestContext(scopes: new string[] { url + "/.default" }) { }).Result.Token);

            AdminClient = KustoClientFactory.CreateCslAdminProvider(kustoConnectionStringBuilder);
            Client = KustoClientFactory.CreateCslQueryProvider(kustoConnectionStringBuilder);


            var ingestionUrl = clusterName.ToKustoClusterUrl(ingest: true);
            var ingestionKustoConnectionStringBuilder = new KustoConnectionStringBuilder(ingestionUrl)
                .WithAadTokenProviderAuthentication(() =>
                    azureServiceTokenProvider.GetTokenAsync(new TokenRequestContext(scopes: new string[] { ingestionUrl + "/.default" }) { }).Result.Token);
            IngestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestionKustoConnectionStringBuilder);

        }

        public IKustoQueuedIngestClient IngestClient { get; }
        public ICslAdminProvider AdminClient { get; }
        public ICslQueryProvider Client { get; }
    }
}