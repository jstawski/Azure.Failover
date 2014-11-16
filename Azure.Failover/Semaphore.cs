using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.Failover
{
    public sealed class Semaphore
    {
        private static volatile Semaphore instance;
        private static object syncRoot = new Object();
        private Stores.IStore store;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);
        public delegate Task AsyncEventHandler(object sender, EventArgs e);
        public event EventHandler Run;
        public event AsyncEventHandler RunAsync;

        private Semaphore(Stores.StoreType storeType, int delay) 
        {
            StoreType = storeType;
            Delay = delay;
        }
        public static Semaphore Instance
        {
            get
            {
                if (instance == null)
                {
                    lock(syncRoot)
                    {
                        if (instance == null)
                        {
                            instance = new Semaphore(Stores.StoreType.TableStorage, 1000);
                        }
                    }
                }

                return instance;
            }
        }
        public Stores.StoreType StoreType { get; set; }
        public int InstanceIndex { get; set; }
        public Stores.TableStorageOptions TableStorageOptions { get; set; }
        public int Delay { get; set; }
        public async Task RegisterAsync(string key, string roleInstanceId)
        {
            int instanceIndex = 0;
            if (!int.TryParse(roleInstanceId.Substring(roleInstanceId.LastIndexOf(".") + 1), out instanceIndex)) // On cloud.
            {
                if (!int.TryParse(roleInstanceId.Substring(roleInstanceId.LastIndexOf("_") + 1), out instanceIndex)) // On compute emulator.
                {
                    throw new ArgumentException("Can't decipher Instance Index from the roleInstanceId", "roleInstanceId");
                }
            }
            await RegisterAsync(key, instanceIndex);
        }
        public async Task RegisterAsync(string key, int instanceIndex)
        {
            InstanceIndex = instanceIndex;
            switch(StoreType)
            {
                case Stores.StoreType.TableStorage:
                    if (TableStorageOptions == null)
                    {
                        throw new ApplicationException("TableStorageOptions is not set");
                    }
                    store = new Stores.TableStorageStore(TableStorageOptions, Delay * 2, Delay * 8);
                    break;
                default:
                    throw new ApplicationException(String.Format("{0} not implemented yet"));
            }
            await store.SetupAsync(key, instanceIndex);

            try
            {
                await this.RunSemaphoreAsync(this.cancellationTokenSource.Token);
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }
        public void Stop()
        {
            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();
        }
        private async Task RunSemaphoreAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var canRun = await store.CanRunAsync();
                if (canRun)
                {
                    if (Run != null)
                    {
                        Run(this, EventArgs.Empty);
                    }
                    if (RunAsync != null)
                    {
                        await RunAsync(this, EventArgs.Empty);
                    }
                    await Task.Delay(Delay);
                }
            }
        }
    }
}