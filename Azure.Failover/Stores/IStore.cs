using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Failover.Stores
{
    interface IStore
    {
        Task SetupAsync(string key, int index);

        Task<bool> CanRunAsync();
    }
}
