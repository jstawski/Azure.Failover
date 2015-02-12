using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Failover.Stores
{
    class BlobStorageStore : IStore
    {
        CloudBlobContainer container = null;
        string connectionString, containerName, key;
        int instanceIndex, idleTimeOut;
        string leaseId = null;
        string tracePrefix = "Azure.FailOver - BlobStorageStore";
        public BlobStorageStore(BlobStorageOptions options)
        {
            connectionString = options.ConnectionString;
            containerName = options.ContainerName;
            if (options.IdleTimeOut < 15000 || options.IdleTimeOut > 60000)
            {
                throw new ArgumentOutOfRangeException("idleTimeOut", "The Idle Time Out must be between 15000 and 60000 milliseconds");
            }
            idleTimeOut = options.IdleTimeOut;
        }

        public async Task SetupAsync(string key, int instanceIndex)
        {
            //Setup the Table Storage 
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            container = blobClient.GetContainerReference(containerName);

            //Check whether the table exists, if not add it
            await container.CreateIfNotExistsAsync();
            this.key = key;
            this.instanceIndex = instanceIndex;
        }

        public async Task<bool> CanRunAsync()
        {
            if (container == null)
            {
                throw new ApplicationException("You must call SetupAsync first!");
            }
            var blobReference = container.GetBlockBlobReference(key);
            if (!blobReference.Exists())
            {
                try
                {
                    await blobReference.UploadFromByteArrayAsync(new byte[0], 0, 0, AccessCondition.GenerateIfNoneMatchCondition("*"), new BlobRequestOptions(), new OperationContext());
                }
                catch(StorageException ex)
                {
                    Trace.TraceError("{0} - {1}: Error Uploading blob: {2}", DateTime.UtcNow, tracePrefix, ex.Message);
                }
            }
            try
            {
                var ts = TimeSpan.FromMilliseconds(this.idleTimeOut);
                leaseId = await blobReference.AcquireLeaseAsync(ts, leaseId);
                if (leaseId != null)
                {
                    return true;
                }
            }
            catch (StorageException ex)
            {
                Trace.TraceError("{0} - {1}: Error Aquiring Lease: {2}", DateTime.UtcNow, tracePrefix, ex.Message);
            }

            return false;
        }

        public async Task<bool> CleanUpAsync()
        {
            var blobReference = container.GetBlockBlobReference(key);
            try
            {
                if (!String.IsNullOrEmpty(leaseId))
                {
                    await blobReference.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(leaseId));
                }
            }
            catch(StorageException ex)
            {
                Trace.TraceError("{0} - {1}: Error Releasing Lease: {2}", DateTime.UtcNow, tracePrefix, ex.Message);
            }
            finally
            {
                leaseId = null;
            }
            return true;
        }
    }
}
