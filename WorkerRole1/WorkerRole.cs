using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;

namespace WorkerRole1
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.TraceInformation("WorkerRole1 is running");
            try
            {
                //Azure.Failover.Semaphore.Instance.Run += Semaphore_Run;
                Azure.Failover.Semaphore.Instance.RunAsync += Semaphore_RunAsync;
                Azure.Failover.Semaphore.Instance.TableStorageOptions = new Azure.Failover.Stores.TableStorageOptions
                {
                    ConnectionString = CloudConfigurationManager.GetSetting("SemaphoreConnectionString")
                };
                Azure.Failover.Semaphore.Instance.Delay = 1000; //It sleeps the thread every 1000 milliseconds. Default value is 1000 milliseconds if not specified.
                Azure.Failover.Semaphore.Instance.RegisterAsync("dis.workerrole.test", RoleEnvironment.CurrentRoleInstance.Id).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }
        int count = 0;
        async Task Semaphore_RunAsync(object sender, EventArgs e)
        {
            count++;
            if (count == 10)
            {
                count = 0;
                Trace.TraceError("Simulating crash - sleeping for 20 seconds");
                await Task.Delay(20 * 1000);
            }
            else
            {
                await Task.Delay(100);
            }
            
            Trace.TraceError("{0}: ---------------> Working from Async {1} <---------------", DateTime.UtcNow, Azure.Failover.Semaphore.Instance.InstanceIndex);
        }

        void Semaphore_Run(object sender, EventArgs e)
        {
            //If you have more than one instance running only one instance at a time will execute this event
            //The method that calls it runs on a loop sleeping the thread on a delay specified.
            Trace.TraceError("---------------> Working {0} <---------------", Azure.Failover.Semaphore.Instance.InstanceIndex);
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("WorkerRole1 has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("WorkerRole1 is stopping");

            Azure.Failover.Semaphore.Instance.Stop();
            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();
            
            base.OnStop();

            Trace.TraceInformation("WorkerRole1 has stopped");
        }
    }
}
