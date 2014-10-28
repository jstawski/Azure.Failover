using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Failover.Stores
{
    public class TableStorageOptions
    {
        public TableStorageOptions()
        {
            TableName = "Semaphore";
        }

        public string ConnectionString { get; set; }
        public string TableName { get; set; }
    }
}
