using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.DI.Core;
using Akka.Event;
using Akka.Routing;
using Autofac;

namespace ConsistentHashingPoolExample
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var builder = new Autofac.ContainerBuilder();
            builder.RegisterType<ProcessUserSessionActor>();
            var container = builder.Build();

            using var system = ActorSystem.Create("MySystem");
            system.UseAutofac(container);

            var brain = system.ActorOf<SessionManager>();

            var messages = new ProcessUserSession[100];
            messages.AsSpan().Fill(new ProcessUserSession());

            var random = new Random();
            for (var i = 0; i < 500; i++)
            {
                var message = messages[random.Next(0, messages.Length)];
                brain.Tell(message);
            }

            // wait for all message processing
            await Task.Delay(4000);
            Console.WriteLine("DONE -- Press any key to terminate.");
            Console.ReadKey();
        }
    }

    public class ProcessUserSession
    {
        public Guid UserId { get; } = Guid.NewGuid();

        public override string ToString() => UserId.ToString();
    }

    public class SessionManager : ReceiveActor
    {
        public SessionManager()
        {
            var props = Context.System.DI().Props<ProcessUserSessionActor>()
                .WithRouter(new ConsistentHashingPool(10)
                    .WithHashMapping(b => ((ProcessUserSession)b).UserId.ToString()));

            Receive<ProcessUserSession>(a =>
            {
                var userSessionProcessor = Context.ActorOf(props);
                userSessionProcessor.Tell(a);
            });
        }
    }

    public class ProcessUserSessionActor : ReceiveActor
    {
        private readonly ILoggingAdapter _log;

        public ProcessUserSessionActor()
        {
            _log = Context.GetLogger();

            Receive<ProcessUserSession>(msg =>
            {
                _log.Info($"ProcessUserSession. GUID: {msg.UserId}");
            });
        }
    }
}
