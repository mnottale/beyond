using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using McMaster.Extensions.CommandLineUtils;

namespace Beyond
{
    class Program
    {
        [Option("--mount")]
        public string Mount { get; }
        [Option("--serve")]
        public string Serve { get; }
        [Option("--create")]
        public bool Create { get; }
        [Option("--key")]
        public string Key { get; }
        [Option("--peers")]
        public string Peers { get; }
        [Option("--client-cert")]
        public string ClientCert { get; }
        [Option("--port")]
        public int Port { get; }
        [Option("--replication")]
        public int Replication { get; } = 1;
        static void Main(string[] args) => CommandLineApplication.Execute<Program>(args);
        
        protected async Task OnExecuteAsync(CancellationToken token)
        {
            GrpcEnvironment.SetLogger(new Grpc.Core.Logging.ConsoleLogger());
            if (!string.IsNullOrEmpty(Mount))
            {
                string key = null;
                string cert = null;
                string rootCA = null;
                if (!string.IsNullOrEmpty(Key))
                {
                    var kr = Key.Split(',');
                    key = File.ReadAllText(kr[0]);
                    var certName = kr[0].Substring(0, kr[0].Length-4)+".crt";
                    cert = File.ReadAllText(certName);
                    rootCA = File.ReadAllText(kr[1]);
                }
                var mountPoint = Mount;
                var serverAddress = Peers;
                Console.WriteLine($"Will mount on {mountPoint} and connect to {serverAddress}");
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
                if (Create)
                    fs.MkFS();
                fs.Run(mountPoint, new string[] {});
                return;
            }
            else if (!string.IsNullOrEmpty(Serve))
            {
                // node mode
                string nodeKey = null;
                string nodeCert = null;
                string nodeRootCA = null;
                string clientCert = null;
                if (!string.IsNullOrEmpty(Key))
                {
                    var kr = Key.Split(',');
                    nodeKey = File.ReadAllText(kr[0]);
                    var certName = kr[0].Substring(0, kr[0].Length-4)+".crt";
                    nodeCert = File.ReadAllText(certName);
                    nodeRootCA = File.ReadAllText(kr[1]);
                }
                if (!string.IsNullOrEmpty(ClientCert))
                {
                    clientCert = File.ReadAllText(ClientCert);
                }
                string path = Serve;

                var service = new BeyondServiceImpl(path, new List<string>{"localhost"}, Port, Replication);
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
                _ = service.Connect(Peers);
                Console.WriteLine("RouteGuide server listening on port " + Port);
                Console.WriteLine("Press any key to stop the server...");
                Console.ReadKey();
                    
                server.ShutdownAsync().Wait();
                server2.ShutdownAsync().Wait();
            }
            else
                throw new System.Exception("--serve or --mount must be specified");
        }
    }
}