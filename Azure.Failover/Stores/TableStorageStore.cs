using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Failover.Stores
{
    class TableStorageStore : IStore
    {
        CloudTable table = null;
        string connectionString, tableName;
        int timeOut;
        Models.TableItem item;
        public TableStorageStore(TableStorageOptions options, int timeOut)
        {
            connectionString = options.ConnectionString;
            tableName = options.TableName;
            this.timeOut = timeOut;
        }

        public async Task SetupAsync(string key, int instanceIndex)
        {
            //Setup the Table Storage 
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference(tableName);

            //Check whether the table exists, if not add it
            await table.CreateIfNotExistsAsync();

            item = new Models.TableItem(key);
            item.InstanceIndex = instanceIndex;
        }

        public async Task<bool> CanRunAsync()
        {
            if (table == null)
            {
                throw new ApplicationException("You must call SetupAsync first!");
            }

            return await TryToClaimSpot();
        }

        private async Task<bool> TryToClaimSpot()
        {
            // Grab the current item
            var retrieveOperation = TableOperation.Retrieve<Models.TableItem>(item.PartitionKey, item.RowKey);
            try
            {
                var retrieveResult = await table.ExecuteAsync(retrieveOperation);

                if (retrieveResult.Result != null)
                {
                    var retrievedItem = (Models.TableItem)retrieveResult.Result;
                    // If the retrieved item is this one, extend the timeout
                    if (retrievedItem.InstanceIndex == item.InstanceIndex)
                    {
                        retrievedItem.Expiration = DateTime.UtcNow.AddMilliseconds(timeOut);
                        var updateOperation = TableOperation.Replace(retrievedItem);
                        try
                        {
                            var updateResult = await table.ExecuteAsync(updateOperation);
                            if (updateResult.HttpStatusCode != (int)HttpStatusCode.NoContent)
                            {
                                return false;
                            }
                            else
                            {
                                return true;
                            }
                        }
                        catch
                        {
                            return false;
                        }

                    }

                    // Someone else had claimed the spot, see if it's expired
                    if (retrievedItem.Expiration > DateTime.UtcNow)
                    {
                        return false;
                    }

                    // It has expired, delete it
                    TableOperation deleteOperation = TableOperation.Delete(retrievedItem);

                    try
                    {
                        var deleteResult = await table.ExecuteAsync(deleteOperation);
                        if (deleteResult.HttpStatusCode != (int)HttpStatusCode.NoContent)
                        {
                            return false;
                        }
                    }
                    catch
                    {
                        return false;
                    }
                }

                // If we got here, then it was because no one had claimed the spot before, or an expired item was deleted
                // Try to claim the spot by inserting it, if someone else inserts it before then it will fail and that's ok
                item.Expiration = DateTime.UtcNow.AddMilliseconds(timeOut);
                TableOperation insertOperation = TableOperation.Insert(item);

                try
                {
                    var insertResult = await table.ExecuteAsync(insertOperation);
                    if (insertResult.Result == null)
                    {
                        return false;
                    }
                }
                catch
                {
                    return false;
                }

                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
