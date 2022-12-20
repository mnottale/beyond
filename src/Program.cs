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
        [Option("--createKey")]
        public string CreateKey { get; }
        [Option("--mountKey")]
        public string MountKey { get; }
        [Option("--mount")]
        public string Mount { get; }
        [Option("--serve")]
        public string Serve { get; }
        [Option("--evict")]
        public string Evict { get; }
        [Option("--heal")]
        public bool Heal { get; }
        [Option("--create")]
        public bool Create { get; }
        [Option("--yes")]
        public bool Yes { get; }
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
        [Option("--uid")]
        public uint Uid { get; }
        [Option("--gid")]
        public uint Gid { get; }
        static void Main(string[] args) => CommandLineApplication.Execute<Program>(args);
        
        public string GetPassword()
        {
            var password = "";
            var ch = Console.ReadKey(true);
            while (ch.Key != ConsoleKey.Enter)
            {
                password += ch.KeyChar;
                Console.Write('*');
                ch = Console.ReadKey(true);
            }
            Console.WriteLine();
            return password;
        }
        protected async Task OnExecuteAsync(CancellationToken token)
        {
            var logger = Logger.loggerFactory.CreateLogger<Program>();
            if (!string.IsNullOrEmpty(CreateKey))
            {
                Console.WriteLine("Please enter a passhrase for your key:");
                var passphrase = GetPassword();
                Console.WriteLine("Please retype the passhrase for confirmation:");
                var confirm = GetPassword();
                if (passphrase != confirm)
                {
                    Console.WriteLine("Mismatch, exiting...(looser)");
                    return;
                }
                var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                var beyond = home + "/.beyond";
                Directory.CreateDirectory(beyond);
                var target = beyond + "/"+CreateKey+".key";
                if (File.Exists(target))
                {
                    Console.WriteLine("File exists, refusing to overwrite");
                    return;
                }
                var k = Crypto.MakeAsymmetricKey(passphrase);
                File.WriteAllBytes(target, k);
                Console.WriteLine("Key saved at " + target);
                return;
            }
            Crypto crypto = null;
            if (!string.IsNullOrEmpty(MountKey))
            {
                crypto = new Crypto();
                var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                var beyond = home + "/.beyond";
                var target = beyond + "/"+MountKey+".key";
                var data = File.ReadAllBytes(target);
                Console.WriteLine("Enter passphrase for " + target);
                var passphrase = GetPassword();
                crypto.SetOwner(data, passphrase);
            }
            //GrpcEnvironment.SetLogger(new Grpc.Core.Logging.ConsoleLogger());
            if (!string.IsNullOrEmpty(Mount)
                ||!string.IsNullOrEmpty(Evict)
                || Heal)
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
                if (!string.IsNullOrEmpty(Mount))
                {
                    var fs = new FileSystem(bclient, Uid, Gid, crypto);
                    if (!string.IsNullOrEmpty(FsName))
                        fs.SetFilesystem(FsName);
                    if (!string.IsNullOrEmpty(MountKey))
                    { // always try to insert key
                        var toput = crypto.ExportOwnerPublicKey();
                        Console.WriteLine("You are using key "  + Utils.KeyString(toput.Key));
                        try
                        {
                            var blk = await bclient.QueryAsync(toput.Key);
                            if (blk.Raw == null || blk.Raw.Length == 0)
                                await bclient.InsertAsync(toput);
                        }
                        catch (Exception e)
                        {
                            logger.LogInformation(e, "Not pushing key");
                        }
                    }
                    if (Create)
                    {
                        if (!Yes)
                        {
                            Console.WriteLine("Please confirm new filesystem creation by typing yes. Anything else will exit.");
                            var confirm = Console.ReadLine();
                            if (confirm != "yes")
                            {
                                Console.WriteLine("Aborting...");
                                return;
                            }
                        }
                        fs.MkFS();
                    }
                    fs.Run(mountPoint, new string[] {});
                }
                else if (!string.IsNullOrEmpty(Evict))
                    await bclient.EvictAsync(Utils.StringKey(Evict));
                else if (Heal)
                    await bclient.HealAsync(new Void());
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
                               logger.LogError(e, "bronk connecting"); 
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