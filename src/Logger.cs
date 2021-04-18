using Microsoft.Extensions.Logging;

namespace Beyond
{
    public static class Logger
    {
       public static ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    //.AddConfiguration(loggingConfiguration.GetSection("Logging"))
                    .AddFilter("Microsoft", LogLevel.Information)
                    .AddFilter("System", LogLevel.Information)
                    .AddFilter("Grpc", LogLevel.Debug)
                    //.AddFilter("SampleApp.Program", LogLevel.Debug)
                    .AddConsole();
                    //.AddEventLog();
            });
    }
}