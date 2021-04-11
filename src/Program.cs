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
            string mode = args[0];
            if (mode == "mount")
            {
                bool create = false;
                int argp = 1;
                if (args[argp] == "--create")
                {
                    create = true;
                    argp++;
                }
                var mountPoint = args[argp++];
                var serverAddress = args[argp++];
                var fsargs = new string[args.Length-argp];
                Array.Copy(args, argp, fsargs, 0, args.Length-argp);
                var channel = new Channel(serverAddress, ChannelCredentials.Insecure);
                var client = new BeyondService.BeyondServiceClient(channel);
                var fs = new FileSystem(client);
                if (create)
                    fs.MkFS();
                fs.Run(mountPoint, fsargs);
                return;
            }
            string path = args[1];
            int Port = Int32.Parse(args[2]);

            var service = new BeyondServiceImpl(path, new List<string>{"localhost"}, Port, 1);
            Server server = new Server
            {
                Services = { BeyondService.BindService(service) },
                Ports = { new ServerPort("localhost", Port, ServerCredentials.Insecure) }
            };
            server.Start();

            if (args.Length > 3)
            {
                _ = service.Connect(args[3]);
            }
            Console.WriteLine("RouteGuide server listening on port " + Port);
            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}