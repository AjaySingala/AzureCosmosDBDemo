using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace ConsoleAppCosmosDB
{
    class Program
    {
        // Step 1: Create an Azure Cosmos DB account.
        // Step 2: Set up your Visual Studio projec.
        
        #region Constants.

        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = "https://ajscosmosdb.documents.azure.com:443/";
        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = "dQJOF89p9vuzmXqpXYTXSUzeIcVN4j4C0XyNrp1v3FznzF7wbxUtlHoixknaErxv4D5IimOwXUGbMcY66DXuRg==";

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        // The container we will create.
        private Container container;

        // The name of the database and container we will create
        private string databaseId = "FamilyDatabase";
        private string containerId = "FamilyContainer";

        #endregion

        public static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Begin operation...");
                Program p = new Program();
                await p.GetStartedDemoAsync();
            }
            catch (CosmosException cex)
            {
                Exception baseException = cex.GetBaseException();
                Console.WriteLine($"{cex.StatusCode} error occurred: {cex}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex);
            }
            finally
            {
                Console.WriteLine("End of Demo. Press any key to exit...");
                Console.ReadKey();
            }
        }

        public async Task GetStartedDemoAsync()
        {
            // Step 3: Connect to an Azure Cosmos DB account.
            // Create an instance of the Cosmos Client.
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);

            #region If you get "503 service unavailable exception" error

            // If you get a "503 service unavailable exception" error, 
            // it's possible that the required ports for direct connectivity mode 
            // are blocked by a firewall. To fix this issue, either open the 
            // required ports or use the gateway mode connectivity as shown 
            // in the following code:

            //// Create a new instance of the Cosmos Client in Gateway mode
            //this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions()
            //{
            //    ConnectionMode = ConnectionMode.Gateway
            //});

            #endregion

            // Step 4: Create a database.
            await this.CreateDBAsync();

            // Step 5: Create a container.
            await this.CreateContainerAsync();

            // Step 6: Add items to the container.
            await this.AddItemsToContainerAsync();

            // Step 7: Query Azure Cosmos DB resources.
            await this.QueryItemsAsync();

            // Step 8: Replace a JSON item.
            await this.ReplaceFamilyItemAsync();

            // Step 9: Delete item.
            await this.DeleteFamilyItemAsync();

            // Step 10: Delete the database.
            await this.DeleteDatabaseAndCleanupAsync();
        }

        public async Task CreateDBAsync()
        {
            // Create a new DB.
            this.database = await this.cosmosClient
                .CreateDatabaseIfNotExistsAsync(this.databaseId);
            Console.WriteLine($"CosmosDB Database Created :{this.database.Id}");
        }

        public async Task CreateContainerAsync()
        {
            // Create a new Container.
            // Specifiy "/LastName" as the partition key since we're storing family 
            // information, to ensure good distribution of requests and storage.
            this.container = await this.database.CreateContainerIfNotExistsAsync(
                containerId, "/LastName");
            Console.WriteLine($"Created Container: {this.container.Id}");
        }

        public async Task AddItemsToContainerAsync()
        {
            // Create a Family object for the "Smith" family.
            Family smithFamily = new Family
            {
                Id = "Smith.1",
                LastName = "Smith",
                Parents = new Parent[]
                {
                    new Parent { FirstName = "Thomas" },
                    new Parent { FirstName = "Mary Kay" }
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FirstName = "Henriette Thaulow",
                        Gender = "female",
                        Grade = 5,
                        Pets = new Pet[]
                {
                    new Pet { GivenName = "Fluffy" }
                }
                    }
                },
                Address = new Address { State = "WA", County = "King", City = "Seattle" },
                IsRegistered = false
            };

            try
            {
                // Read the item to see if it exists.
                ItemResponse<Family> smithFamilyResponse =
                    await this.container.ReadItemAsync<Family>(smithFamily.Id,
                        new PartitionKey(smithFamily.LastName));
                Console.WriteLine($"Item in database with id: {smithFamilyResponse.Resource.Id} already exists");
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {

                // Create an item in the container representing the Smith family. 
                // Note: we provide the value of the partition key for this item, 
                // which is "Smith".
                ItemResponse<Family> smithFamilyResponse =
                    await this.container.CreateItemAsync<Family>(smithFamily,
                        new PartitionKey(smithFamily.LastName));

                // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n",
                    smithFamilyResponse.Resource.Id, smithFamilyResponse.RequestCharge);
            }

            // Create a Family object for the "Wakefield" family.
            Family wakefieldFamily = new Family
            {
                Id = "Wakefield.7",
                LastName = "Wakefield",
                Parents = new Parent[]
                {
                    new Parent { FamilyName = "Wakefield", FirstName = "Robin" },
                    new Parent { FamilyName = "Miller", FirstName = "Ben" }
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FamilyName = "Merriam",
                        FirstName = "Jesse",
                        Gender = "female",
                        Grade = 8,
                        Pets = new Pet[]
                        {
                            new Pet { GivenName = "Goofy" },
                            new Pet { GivenName = "Shadow" }
                        }
                    },
                    new Child
                    {
                        FamilyName = "Miller",
                        FirstName = "Lisa",
                        Gender = "female",
                        Grade = 1
                    }
                },
                Address = new Address { State = "NY", County = "Manhattan", City = "NY" },
                IsRegistered = true
            };

            try
            {
                // Read the item to see if it exists
                ItemResponse<Family> wakefieldFamilyResponse = 
                    await this.container.ReadItemAsync<Family>(wakefieldFamily.Id,
                        new PartitionKey(wakefieldFamily.LastName));
                Console.WriteLine("Item in database with id: {0} already exists\n", 
                    wakefieldFamilyResponse.Resource.Id);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Create an item in the container representing the Wakefield family. Note we provide the value of the partition key for this item, which is "Wakefield"
                ItemResponse<Family> wakefieldFamilyResponse = 
                    await this.container.CreateItemAsync<Family>(wakefieldFamily, 
                        new PartitionKey(wakefieldFamily.LastName));

                // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", 
                    wakefieldFamilyResponse.Resource.Id, wakefieldFamilyResponse.RequestCharge);
            }
        }

        public async Task QueryItemsAsync()
        {
            var sql = "SELECT * FROM c WHERE c.LastName = 'Smith'";
            Console.WriteLine($"Running query: {sql}\n");

            QueryDefinition queryDef = new QueryDefinition(sql);
            FeedIterator<Family> queryResultSetIterator =
                this.container.GetItemQueryIterator<Family>(queryDef);

            List<Family> families = new List<Family>();
            while(queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Family> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach(Family family in currentResultSet)
                {
                    families.Add(family);
                    Console.WriteLine($"Read {family}\n");
                }
            }
        }

        public async Task ReplaceFamilyItemAsync()
        {
            ItemResponse<Family> wakefieldFamilyResponse =
                await this.container.ReadItemAsync<Family>("Wakefield.7", 
                    new PartitionKey("Wakefield"));
            var itemBody = wakefieldFamilyResponse.Resource;

            // Update the Registration Status.
            itemBody.IsRegistered = true;
            // Upgrade the Child's grade.
            itemBody.Children[0].Grade = 6;

            // Replace the item with the updated content.
            wakefieldFamilyResponse = 
                await this.container.ReplaceItemAsync<Family>(itemBody, 
                    itemBody.Id, 
                    new PartitionKey(itemBody.LastName));

            Console.WriteLine("Updated Family [{0},{1}].\n \tBody is now: {2}\n", 
                itemBody.LastName, itemBody.Id, wakefieldFamilyResponse.Resource);
        }

        public async Task DeleteFamilyItemAsync()
        {
            var partitionKeyValue = "Wakefield";
            var familyId = "Wakefield.7";

            // Delete an item.
            // Note: we must provide the partition key value and id of the item to delete.
            ItemResponse<Family> wakefieldFamilyResponse = 
                await this.container.DeleteItemAsync<Family>(familyId, 
                    new PartitionKey(partitionKeyValue));
            Console.WriteLine("Deleted Family [{0},{1}]\n", partitionKeyValue, familyId);
        }

        private async Task DeleteDatabaseAndCleanupAsync()
        {
            DatabaseResponse databaseResourceResponse = await this.database.DeleteAsync();
            // Also valid: await this.cosmosClient.Databases["FamilyDatabase"].DeleteAsync();

            Console.WriteLine("Deleted Database: {0}\n", this.databaseId);

            //Dispose of CosmosClient
            this.cosmosClient.Dispose();
        }
    }
}
