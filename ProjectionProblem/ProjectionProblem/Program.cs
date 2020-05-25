namespace ProjectionProblem
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using EventStore.ClientAPI;
    using EventStore.ClientAPI.Common.Log;
    using EventStore.ClientAPI.Projections;
    using EventStore.ClientAPI.SystemData;
    using Newtonsoft.Json;

    /// <summary>
    /// 
    /// </summary>
    internal class Program
    {
        #region Fields

        /// <summary>
        /// The ip address
        /// </summary>
        private static readonly IPAddress IpAddress = IPAddress.Parse("127.0.0.1");

        #endregion

        #region Methods

        /// <summary>
        /// Adds the events.
        /// </summary>
        /// <param name="eventStoreConnection">The event store connection.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="numberOfEvents">The number of events.</param>
        private static async Task AddEvents(IEventStoreConnection eventStoreConnection,
                                            String stream,
                                            Int32 numberOfEvents)
        {
            List<EventData> events = new List<EventData>();
            var @event = new
                         {
                             id = Guid.NewGuid()
                         };

            String json = JsonConvert.SerializeObject(@event);

            for (Int32 i = 0; i < numberOfEvents; i++)
            {
                events.Add(new EventData(Guid.NewGuid(), "AddedEvent", true, Encoding.Default.GetBytes(json), null));
            }

            await eventStoreConnection.AppendToStreamAsync(stream, -2, events);
        }

        /// <summary>
        /// Gets the projections manager.
        /// </summary>
        /// <returns></returns>
        private static ProjectionsManager GetProjectionsManager()
        {
            IPEndPoint endPoint = new IPEndPoint(Program.IpAddress, 2113);

            return new ProjectionsManager(new ConsoleLogger(), endPoint, TimeSpan.FromSeconds(30), null, "http");
        }

        /// <summary>
        /// Gets the user credentials.
        /// </summary>
        /// <returns></returns>
        private static UserCredentials GetUserCredentials()
        {
            return new UserCredentials("admin", "changeit");
        }

        /// <summary>
        /// Mains the specified arguments.
        /// </summary>
        /// <param name="args">The arguments.</param>
        private static async Task Main(String[] args)
        {
            // This example assumes you have an event store running locally on the default ports with the default username and password
            // If your event store connection information is different then update this connection string variable to point to your event store
            String connectionString = $"ConnectTo=tcp://admin:changeit@{Program.IpAddress}:1113;VerboseLogging=true;";

            // The subscription service requires an open connection to operate so create and open the connection here
            IEventStoreConnection eventStoreConnection = EventStoreConnection.Create(connectionString);

            await eventStoreConnection.ConnectAsync();

            await Program.AddEvents(eventStoreConnection, "Stream1-1", 1000);
            await Program.AddEvents(eventStoreConnection, "Stream2-1", 1000);

            await Program.StartProjection("TestProjection1");

            //await TruncateStreamTcp(eventStoreConnection, "$ce-Stream1", 100);                     //works
            //await TruncateStreamTcp(eventStoreConnection, "$ce-Stream1", 101);                     //works
            // await Program.TruncateStreamTcp(eventStoreConnection, "$ce-Stream1", 110);            //works

            //Both tcp and http truncate fails from 111

            await Program.TruncateStreamTcp(eventStoreConnection, "$ce-Stream1", 111); //fails
            // await TruncateStreamHttp("$ce-Stream1", 111);      //http truncate also fails

            //This projection no longer works (all stream position at -1)
            await Program.ResetProjection("TestProjection1");

            //This new projection does not work (all stream position at -1)
            await Program.StartProjection("TestProjection2");

            Console.ReadKey();
        }

        /// <summary>
        /// Resets the projection.
        /// </summary>
        /// <param name="projection">The projection.</param>
        private static async Task ResetProjection(String projection)
        {
            var projectionManager = Program.GetProjectionsManager();

            await projectionManager.ResetAsync(projection, Program.GetUserCredentials());

            Console.WriteLine($"Reseting projection {projection}");
        }

        /// <summary>
        /// Scavenges this instance.
        /// </summary>
        private static async Task Scavenge()
        {
            HttpClient httpClient = new HttpClient();
            String uri = $"http://{Program.IpAddress}:2113/admin/scavenge";

            HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Post, uri);

            requestMessage.Headers.Add("Accept", @"application/json");
            requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes("admin:changeit")));

            await httpClient.SendAsync(requestMessage, CancellationToken.None);

            Console.WriteLine("Scavenge started");

            Thread.Sleep(5000);
        }

        /// <summary>
        /// Starts the projection.
        /// </summary>
        /// <param name="projectionName">Name of the projection.</param>
        private static async Task StartProjection(String projectionName)
        {
            var projectionManager = Program.GetProjectionsManager();

            String projection = @"fromStreams('$ce-Stream1','$ce-Stream2')
                .when({
                $init: (s, e) => {
                           return { count: 0}
                       },
      
                'AddedEvent' : (s, e) => {
                                         s.count++;
                                     }
            })";

            try
            {
                await projectionManager.CreateContinuousAsync(projectionName, projection, false, Program.GetUserCredentials());

                Console.WriteLine("Starting projection TestProjection1");
            }
            catch
            {
                //silently handle for now
            }
        }

        /// <summary>
        /// Truncates the stream HTTP.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="truncateBefore">The truncate before.</param>
        private static async Task TruncateStreamHttp(String stream,
                                                     Int32 truncateBefore)
        {
            HttpClient httpClient = new HttpClient();
            HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Post, $"http://{Program.IpAddress}:2113/streams/{stream}/metadata");

            requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes("admin:changeit")));

            String payload = "[{\"eventId\":\"16768949-8949-8949-8949-159016768949\",\"eventType\":\"truncate\",\"data\":{\"$tb\":5}}]";

            payload = payload.Replace(":5", $":{truncateBefore}");

            requestMessage.Content = new StringContent(payload, Encoding.UTF8, "application/vnd.eventstore.events+json");

            await httpClient.SendAsync(requestMessage, CancellationToken.None);

            Console.WriteLine($"truncate stream http {stream}");
        }

        /// <summary>
        /// Truncates the stream TCP.
        /// </summary>
        /// <param name="eventStoreConnection">The event store connection.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="truncateBefore">The truncate before.</param>
        private static async Task TruncateStreamTcp(IEventStoreConnection eventStoreConnection,
                                                    String stream,
                                                    Int32 truncateBefore)
        {
            await eventStoreConnection.SetStreamMetadataAsync(stream, -2, StreamMetadata.Create(truncateBefore:truncateBefore));

            Console.WriteLine($"truncate stream tcp {stream}");
        }

        #endregion
    }
}