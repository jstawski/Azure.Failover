using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Failover.Stores
{
    public enum StoreType : byte
    {
        TableStorage = 0,
        Redis = 1,
        ManagedCache = 2,
        DocumentDb = 3
    }
}
