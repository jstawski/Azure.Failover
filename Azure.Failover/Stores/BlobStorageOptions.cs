using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Azure.Failover.Stores
{
    public class BlobStorageOptions
    {
        public BlobStorageOptions()
        {
            ContainerName = "semaphore";
            IdleTimeOut = 15000;
        }

        public string ConnectionString { get; set; }

        public string ContainerName { get; set; }

        public int IdleTimeOut { get; set; }
    }
}
