using Grpc.Net.Client;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;

namespace Beyond
{
    class Program
    {
        [Option("--mount")]
        public string Mount { get; }
        [Option("--serve")]
        public string Serve { get; }
        [Option("--evict")]
        public string Evict { get; }
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
        [Option("--fs-name")]
        public string FsName { get; }
        static void Main(string[] args) => CommandLineApplication.Execute<Program>(args);
        
        protected async Task OnExecuteAsync(CancellationToken token)
        {
            //GrpcEnvironment.SetLogger(new Grpc.Core.Logging.ConsoleLogger());
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
                GrpcChannel channel = null;
                if (key != null)
                    channel = GrpcChannel.ForAddress(serverAddress, new GrpcChannelOptions
                        {
                            Credentials = new SslCredentials(
                                rootCA,
                                new KeyCertificatePair(cert, key)
                                )
                        });
                else
                    channel = GrpcChannel.ForAddress(serverAddress, new GrpcChannelOptions
                        {
                            Credentials = ChannelCredentials.Insecure,
                        });
                var bclient = new BeyondClient.BeyondClientClient(channel);
                var fs = new FileSystem(bclient);
                if (!string.IsNullOrEmpty(FsName))
                    fs.SetFilesystem(FsName);
                if (Create)
                    fs.MkFS();
                fs.Run(mountPoint, new string[] {});
                return;
            }
            else if (!string.IsNullOrEmpty(Evict))
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
                GrpcChannel channel = null;
                if (key != null)
                    channel = GrpcChannel.ForAddress(Peers, new GrpcChannelOptions
                        {
                            Credentials = new SslCredentials(
                                rootCA,
                                new KeyCertificatePair(cert, key)
                                )
                        });
                else
                    channel = GrpcChannel.ForAddress(Peers, new GrpcChannelOptions
                        {
                            Credentials = ChannelCredentials.Insecure,
                        });
                var bclient = new BeyondClient.BeyondClientClient(channel);
                await bclient.EvictAsync(Utils.StringKey(Evict));
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

                State.replicationFactor = Replication;
                State.rootPath = Serve;
                State.port = Port;
                State.backend = new BeyondServiceImpl(Logger.loggerFactory.CreateLogger<BeyondServiceImpl>());
                //var client = new BeyondClientImpl();
                var builder = WebApplication.CreateBuilder();
                builder.Services.AddGrpc();
                builder.WebHost.UseUrls($"http://localhost:{Port}");
                builder.WebHost.ConfigureKestrel((options) =>
                    {
                        // trying to use Http1AndHttp2 causes http2 connections to fail with invalid protocol error
                        // according to Microsoft dual http version mode not supported in unencrypted scenario: https://learn.microsoft.com/en-us/aspnet/core/grpc/troubleshoot?view=aspnetcore-3.0
                        options.ConfigureEndpointDefaults(lo => lo.Protocols = HttpProtocols.Http2);
                    });
                var app = builder.Build();
                app.MapGrpcService<BeyondServiceImpl>();
                app.MapGrpcService<BeyondClientImpl>();
                if (!string.IsNullOrEmpty(Peers))
                {
                    _ = Task.Delay(100).ContinueWith(async _ =>
                        {
                            try
                            {
                                await State.backend.Connect(Peers);
                            }
                            catch(Exception e)
                            {
                                Logger.loggerFactory.CreateLogger<Program>().LogError(e, "bronk connecting"); 
                            }
                        });
                }
                app.Run();
                /*
                GrpcServerCredentials nodeServerCredentials = ServerCredentials.Insecure;
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
                */
            }
            else
                throw new System.Exception("--serve or --mount must be specified");
        }
    }
}