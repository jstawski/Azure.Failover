using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        int activeTimeOut, idleTimeOut;
        Models.TableItem item;
        bool isRunning = false;
        DateTime expirationDate = DateTime.MinValue;
        public TableStorageStore(TableStorageOptions options, int activeTimeOut, int idleTimeOut)
        {
            connectionString = options.ConnectionString;
            tableName = options.TableName;
            this.activeTimeOut = activeTimeOut;
            this.idleTimeOut = idleTimeOut;
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
            if (!isRunning && ((DateTime.UtcNow - expirationDate).TotalMilliseconds >= idleTimeOut || expirationDate == DateTime.MinValue))
            {
                isRunning = await TryToClaimSpot();
            }
            else if (isRunning)
            {
                Trace.TraceError("{0}: Update Expiration Date", DateTime.UtcNow);
                //update expiration date without caring for the returned result as it should be true!
                await TryToClaimSpot();
            }
            return isRunning;
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
                    Trace.TraceError("{0}: {1} Retrieved item:", DateTime.UtcNow, item.InstanceIndex);
                    Trace.TraceError("{0}: Expiration Date: {1}", DateTime.UtcNow, retrievedItem.Expiration);
                    Trace.TraceError("{0}: Instance Index: {1}", DateTime.UtcNow, retrievedItem.InstanceIndex);
                    expirationDate = retrievedItem.Expiration;
                    // If the retrieved item is this one, extend the timeout
                    if (retrievedItem.InstanceIndex == item.InstanceIndex)
                    {
                        expirationDate = DateTime.UtcNow.AddMilliseconds(activeTimeOut);
                        retrievedItem.Expiration = expirationDate;
                        Trace.TraceError("{0}: Same Instance. Updating expiration date from to {1}", DateTime.UtcNow, retrievedItem.Expiration);
                        var updateOperation = TableOperation.Replace(retrievedItem);
                        try
                        {
                            var updateResult = await table.ExecuteAsync(updateOperation);
                            if (updateResult.HttpStatusCode != (int)HttpStatusCode.NoContent)
                            {
                                Trace.TraceError("{0}: problem updating expiration date", DateTime.UtcNow);
                                return false;
                            }
                            else
                            {
                                Trace.TraceError("{0}: updated expiration date", DateTime.UtcNow);
                                return true;
                            }
                        }
                        catch
                        {
                            Trace.TraceError("{0}: problem updating expiration date", DateTime.UtcNow);
                            return false;
                        }

                    }

                    // Someone else had claimed the spot, see if it's expired
                    if (retrievedItem.Expiration > DateTime.UtcNow)
                    {
                        Trace.TraceError("{0}: Claimed by someone else and hasn't expired", DateTime.UtcNow);
                        return false;
                    }
                    
                    // It has expired, delete it
                    TableOperation deleteOperation = TableOperation.Delete(retrievedItem);

                    try
                    {
                        Trace.TraceError("{0}: Expired! Trying to delete it", DateTime.UtcNow);
                        var deleteResult = await table.ExecuteAsync(deleteOperation);
                        if (deleteResult.HttpStatusCode != (int)HttpStatusCode.NoContent)
                        {
                            Trace.TraceError("{0}: Problem deleting", DateTime.UtcNow);
                            return false;
                        }
                    }
                    catch
                    {
                        Trace.TraceError("{0}: Problem deleting", DateTime.UtcNow);
                        return false;
                    }
                }

                // If we got here, then it was because no one had claimed the spot before, or an expired item was deleted
                // Try to claim the spot by inserting it, if someone else inserts it before then it will fail and that's ok
                expirationDate = DateTime.UtcNow.AddMilliseconds(activeTimeOut);
                item.Expiration = expirationDate;
                TableOperation insertOperation = TableOperation.Insert(item);

                try
                {
                    Trace.TraceError("{0}: Trying to insert item", DateTime.UtcNow);
                    var insertResult = await table.ExecuteAsync(insertOperation);
                    if (insertResult.Result == null)
                    {
                        Trace.TraceError("{0}: Problem inserting", DateTime.UtcNow);
                        return false;
                    }
                }
                catch
                {
                    Trace.TraceError("{0}: Problem inserting", DateTime.UtcNow);
                    return false;
                }

                Trace.TraceError("{0}: Claimed spot", DateTime.UtcNow);
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
