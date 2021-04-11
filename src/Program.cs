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
            string path = args[0];
            int Port = Int32.Parse(args[1]);

            var service = new BeyondServiceImpl(path, new List<string>{"localhost"}, Port, 3);
            Server server = new Server
            {
                Services = { BeyondService.BindService(service) },
                Ports = { new ServerPort("localhost", Port, ServerCredentials.Insecure) }
            };
            server.Start();

            if (args.Length > 2)
            {
                _ = service.Connect(args[2]);
            }
            Console.WriteLine("RouteGuide server listening on port " + Port);
            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}