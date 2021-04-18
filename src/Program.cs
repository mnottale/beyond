using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Beyond
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0 || args[0] == "-h" || args[0] == "--help")
            {
                Console.WriteLine(@"Usage:
  beyond mount [--create] [--key KEY_FILE ROOT_CA] MOUNTPOINT NODE_HOST:NODE_PORT
  beyond serve [--node-key KEY_FILE ROOT_CA_FILE] [--client-cert CERT_FILE] ROOT_PATH PORT
                    ");
                return;
            }
            GrpcEnvironment.SetLogger(new Grpc.Core.Logging.ConsoleLogger());
            string mode = args[0];
            int argp = 1;
            if (mode == "mount")
            {
                bool create = false;
                if (args[argp] == "--create")
                {
                    create = true;
                    argp++;
                }
                string key = null;
                string cert = null;
                string rootCA = null;
                if (args[argp] == "--key")
                {
                    argp++;
                    key = File.ReadAllText(args[argp]);
                    var certName = args[argp].Substring(0, args[argp].Length-4)+".crt";
                    cert = File.ReadAllText(certName);
                    argp++;
                    rootCA = File.ReadAllText(args[argp++]);
                }
                var mountPoint = args[argp++];
                var serverAddress = args[argp++];
                Console.WriteLine($"Will mount on {mountPoint} and connect to {serverAddress}");
                var fsargs = new string[args.Length-argp];
                Array.Copy(args, argp, fsargs, 0, args.Length-argp);
                Channel channel = null;
                if (key != null)
                    channel = new Channel(serverAddress,
                        new SslCredentials(
                            rootCA,
                            new KeyCertificatePair(cert, key)
                            )
                        );
                else
                    channel = new Channel(serverAddress, ChannelCredentials.Insecure);
                var bclient = new BeyondClient.BeyondClientClient(channel);
                var fs = new FileSystem(bclient);
                if (create)
                    fs.MkFS();
                fs.Run(mountPoint, fsargs);
                return;
            }
            // node mode
            string nodeKey = null;
            string nodeCert = null;
            string nodeRootCA = null;
            string clientCert = null;
            if (args[argp] == "--node-key")
            {
                argp++;
                nodeKey = File.ReadAllText(args[argp]);
                var certName = args[argp].Substring(0, args[argp].Length-4)+".crt";
                nodeCert = File.ReadAllText(certName);
                argp++;
                nodeRootCA = File.ReadAllText(args[argp++]);
            }
            if (args[argp] == "--client-cert")
            {
                argp++;
                clientCert = File.ReadAllText(args[argp++]);
            }
            string path = args[argp++];
            int Port = Int32.Parse(args[argp++]);

            var service = new BeyondServiceImpl(path, new List<string>{"localhost"}, Port, 1);
            var client = new BeyondClientImpl(service);
            ServerCredentials nodeServerCredentials = ServerCredentials.Insecure;
            if (nodeKey != null)
            {
                nodeServerCredentials = new SslServerCredentials(
                    new List<KeyCertificatePair> { new KeyCertificatePair(nodeCert, nodeKey) },
                    nodeRootCA,
                    SslClientCertificateRequestType.RequestAndRequireAndVerify
                    );
            }
            Server server = new Server
            {
                Services = { BeyondNode.BindService(service) },
                Ports = { new ServerPort("localhost", Port, nodeServerCredentials) }
            };
            server.Start();

            ServerCredentials clientServerCredentials = ServerCredentials.Insecure;
            if (clientCert != null)
            {
                clientServerCredentials = new SslServerCredentials(
                    new List<KeyCertificatePair> {new KeyCertificatePair(nodeCert, nodeKey) },
                    clientCert,
                    SslClientCertificateRequestType.RequestAndRequireAndVerify
                    );
            }
            Server server2 = new Server
            {
                Services = { BeyondClient.BindService(client) },
                Ports = { new ServerPort("localhost", Port+1, clientServerCredentials) }
            };

            server2.Start();
            if (args.Length > 3)
            {
                _ = service.Connect(args[3]);
            }
            Console.WriteLine("RouteGuide server listening on port " + Port);
            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
            server2.ShutdownAsync().Wait();
        }
    }
}