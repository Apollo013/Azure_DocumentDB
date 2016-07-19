using DocumentDB.Models;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace DocumentDB
{
    /// <summary>
    /// Demonstrates the basics of DocumentDB
    /// </summary>
    class Program
    {

        // These settings should be stored in app settings
        private const string EndpointUri = "[ENTER_ENPOINT_URL_HERE]";
        private const string PrimaryKey = "[ENTER_PRIMARY_KEY_HERE";
        private DocumentClient client;

        static void Main(string[] args)
        {
            try
            {
                Program p = new Program();
                p.PrintTitle("DocumentDB");
                Task.Run(() => p.CreateClient()).Wait();
            }
            catch (DocumentClientException ex)
            {
                Exception baseException = ex.GetBaseException();
                Console.WriteLine($"{ex.StatusCode} error occurred: {ex.Message}, Message: {baseException.Message}");
            }
            catch (Exception ex)
            {
                Exception baseException = ex.GetBaseException();
                Console.WriteLine($"Error: {ex.Message}, Message: { baseException.Message}");
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Creates a new Document Client Object, Database, Collection and 2 documents
        /// </summary>
        /// <returns></returns>
        private void CreateClient()
        {
            PrintSubTitle("Creating Client");
            this.client = new DocumentClient(new Uri(EndpointUri), PrimaryKey);

            this.CreateDatabaseAsync("FamilyDB").Wait();
            this.CreateCollectionAsync("FamilyDB", "FamilyCollection").Wait();
            this.CreateFamilyDocument("FamilyDB", "FamilyCollection", CreateAndersonFamily()).Wait();
            this.CreateFamilyDocument("FamilyDB", "FamilyCollection", CreateWakefieldFamily()).Wait();
            this.ExecuteQuery("FamilyDB", "FamilyCollection").Wait();

            Family family = CreateAndersonFamily();
            family.Children[0].Grade = 6;
            this.ReplaceDocument("FamilyDB", "FamilyCollection", family).Wait();

            this.ExecuteQuery("FamilyDB", "FamilyCollection").Wait();

            this.RemoveDocument("FamilyDB", "FamilyCollection", family.Id).Wait();

            this.ExecuteQuery("FamilyDB", "FamilyCollection").Wait();

        }


        private async Task DeleteDatabase(string databaseName)
        {
            Database database = client
            .CreateDatabaseQuery()
            .Where((d) => d.Id == databaseName)
            .AsEnumerable()
            .First();
            if (database != null)
            {
                await client.DeleteDatabaseAsync(database.SelfLink);
            }
            PrintSubTitle("Deleted");
        }

        /// <summary>
        /// Creates a database (checks first if it exists)
        /// </summary>
        /// <param name="dbName"></param>
        /// <returns></returns>
        private async Task CreateDatabaseAsync(string dbName)
        {
            try
            {
                // Try to read from the database first
                await this.client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(dbName));
                PrintSubTitle("Database");
                this.WriteToConsoleAndPromptToContinue($"Found: {dbName}");
            }
            catch (DocumentClientException ex)
            {
                // Create the database if it does not exist
                if (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    await this.client.CreateDatabaseAsync(new Database { Id = dbName });
                    PrintSubTitle("Database");
                    this.WriteToConsoleAndPromptToContinue($"Created: {dbName}");
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Creates a collection (checks first if it exists)
        /// </summary>
        /// <param name="dbName"></param>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        private async Task CreateCollectionAsync(string dbName, string collectionName)
        {

            try
            {
                await this.client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(dbName, collectionName));
                PrintSubTitle("Collection");
                this.WriteToConsoleAndPromptToContinue($"Found: {collectionName}");
            }
            catch (DocumentClientException ex)
            {
                // Create the collection if it does not exist
                if (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    DocumentCollection dc = new DocumentCollection();
                    dc.Id = collectionName;

                    // Configure collections for maximum query flexibility including string range queries.
                    dc.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });

                    // Here we create a collection with 10 RU/s.
                    await this.client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(dbName), dc, new RequestOptions { OfferThroughput = 400 });
                    PrintSubTitle("Collection");
                    this.WriteToConsoleAndPromptToContinue($"Created: {collectionName}");
                }
                else
                {
                    throw;
                }
            }
        }


        /// <summary>
        /// Creates a Family Document (checks first if it exists)
        /// </summary>
        /// <param name="dbName"></param>
        /// <param name="collectionName"></param>
        /// <param name="family"></param>
        /// <returns></returns>
        private async Task CreateFamilyDocument(string dbName, string collectionName, Family family)
        {
            try
            {
                await this.client.ReadDocumentAsync(UriFactory.CreateDocumentUri(dbName, collectionName, family.Id));
                PrintSubTitle($"Document: {family.Id}");
                this.WriteToConsoleAndPromptToContinue($"Found: {family.Id}");
            }
            catch (DocumentClientException de)
            {
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await this.client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(dbName, collectionName), family);
                    PrintSubTitle($"Document: {family.Id}");
                    this.WriteToConsoleAndPromptToContinue($"Created: {family.Id}");
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Executes a predefined query
        /// </summary>
        /// <param name="dbName"></param>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        private async Task ExecuteQuery(string dbName, string collectionName)
        {
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = 100 };

            // -------------------------------------------------------       
            // SYNCHRONOUS            
            // -------------------------------------------------------  
            /*     
            IQueryable<Family> familyQuery = 
                this.client.CreateDocumentQuery<Family>(UriFactory.CreateDocumentCollectionUri(dbName, collectionName), queryOptions)
                .Where(f => f.LastName == "Andersen");

            // Process the results            
            foreach (Family family in familyQuery)
            {
                PrintSubTitle("Running Query Synchronously");
                Console.WriteLine($"\tSynchronous Read {family}");
            }
            */
            // -------------------------------------------------------       
            // ASYNCHRONOUS     
            // -------------------------------------------------------   

            IDocumentQuery<Family> familyQueryAsync = (from family in this.client.CreateDocumentQuery<Family>(UriFactory.CreateDocumentCollectionUri(dbName, collectionName), queryOptions)
                                                       where family.LastName == "Andersen"
                                                       select family)
                                                       .AsDocumentQuery();

            // Process the results            
            while (familyQueryAsync.HasMoreResults)
            {
                FeedResponse<Family> family = await familyQueryAsync.ExecuteNextAsync<Family>();

                if (family.Any())
                {
                    Family doc = family.Single();
                    this.PrintSubTitle($"Running Query Asynchronously: {doc.Id}");
                }
                else
                {
                    this.PrintSubTitle($"Nothing Found");
                }
            }

            // -------------------------------------------------------       
            // DIRECT SQL    
            // -------------------------------------------------------               
            /*
            IQueryable<Family> familySqlQuery = this.client.CreateDocumentQuery<Family>(UriFactory.CreateDocumentCollectionUri(dbName, collectionName),
                "SELECT * FROM Family Where Family.lastName = 'Andersen'",
                queryOptions);

            // Process the results            
            foreach (Family family in familySqlQuery)
            {
                this.PrintSubTitle($"Running Query Synchronously with SQL: {family.Id}");
            }
            */
        }

        /// <summary>
        /// Replaces details of a document
        /// </summary>
        /// <param name="dbName"></param>
        /// <param name="collectionName"></param>
        /// <param name="family"></param>
        /// <returns></returns>
        private async Task ReplaceDocument(string dbName, string collectionName, Family family)
        {
            try
            {
                await this.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(dbName, collectionName, family.Id), family);
                this.PrintSubTitle($"Document Replaced: {family.Id}");
            }
            catch (DocumentClientException ex)
            {
                throw;
            }
        }

        /// <summary>
        /// Removes a Document
        /// </summary>
        /// <param name="dbName"></param>
        /// <param name="collectionName"></param>
        /// <param name="documentName"></param>
        /// <returns></returns>
        private async Task RemoveDocument(string dbName, string collectionName, string documentName)
        {
            try
            {
                await this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(dbName, collectionName, documentName));
                this.PrintSubTitle($"Document Removed: {documentName}");
            }
            catch (DocumentClientException ex)
            {
                throw;
            }
        }

        private Family CreateAndersonFamily()
        {
            return new Family
            {
                Id = "Andersen.1",
                LastName = "Andersen",
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
                IsRegistered = true
            };
        }

        private Family CreateWakefieldFamily()
        {
            return new Family
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
                IsRegistered = false
            };
        }


        #region PRINT METHODS
        /// <summary>
        /// Helper method for writing to the console
        /// </summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        private void WriteToConsoleAndPromptToContinue(string format, params object[] args)
        {
            Console.WriteLine(format, args);
            //Console.WriteLine("Press any key to continue ...");
            //Console.ReadKey();
        }

        private void PrintTitle(string title)
        {
            Console.WriteLine("\n");
            Console.WriteLine("***************************************************");
            Console.WriteLine(title);
            Console.WriteLine("***************************************************");
        }

        private void PrintSubTitle(string title)
        {
            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine("===============================================================================================================================");
            Console.WriteLine(title);
            Console.WriteLine("===============================================================================================================================");
        }
        #endregion

    }
}
