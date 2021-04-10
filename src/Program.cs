using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Beyond
{
    class Program
    {
        static void Main(string[] args)
        {
            int Port = Int32.Parse(args[0]);

            Storage storage = new Storage("/tmp/beyond-store");
            var service = new BeyondServiceImpl(storage, new List<string>{"localhost"}, Port, 3);
            Server server = new Server
            {
                Services = { BeyondService.BindService(service) },
                Ports = { new ServerPort("localhost", Port, ServerCredentials.Insecure) }
            };
            server.Start();

            if (args.Length > 1)
            {
                _ = service.Connect(args[1]);
            }
            Console.WriteLine("RouteGuide server listening on port " + Port);
            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}