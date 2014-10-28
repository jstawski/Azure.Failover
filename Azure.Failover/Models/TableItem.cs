using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Failover.Models
{
    internal class TableItem : TableEntity
    {
        internal TableItem(string key)
        {
            PartitionKey = key;
            RowKey = key;
        }

        public TableItem() { }

        public int InstanceIndex { get; set; }
        public DateTime Expiration { get; set; }
    }
}
