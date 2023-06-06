/**
 * @file Consumes messages from the RDI DLQ, writes to disk and deletes from the DLQ.
 * @author Michael Yuan
**/

using System.Globalization;
using StackExchange.Redis;

ConfigurationOptions config = new ConfigurationOptions
{
    EndPoints = { "redis-12001.yuan.demo.redislabs.com:12001" },
    Password = ""
};

var tokenSource = new CancellationTokenSource();
var token = tokenSource.Token;
var muxer = ConnectionMultiplexer.Connect(config);
var db = muxer.GetDatabase();

const string streamName = "dlq:data:transaction:{06S}";

Dictionary<string, string> ParseResult(StreamEntry entry) => entry.Values.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString());

var readTask = Task.Run(async () =>
{
    while (!token.IsCancellationRequested)
    {
        var result = await db.StreamRangeAsync(streamName, "-", "+", 1, Order.Descending);
        if (result.Any())
        {
            StreamEntry msg = result.First();
            var dict = ParseResult(msg);

            foreach (KeyValuePair<string, string> field in dict)
            {
                Console.WriteLine(msg.Id);
                Console.WriteLine(field.Key);
                Console.WriteLine(field.Value);

                using (StreamWriter writer = new StreamWriter("./dlq.out",true))
                {
                    writer.WriteLine(msg.Id);
                    writer.WriteLine(field.Key);
                    writer.WriteLine(field.Value);
                }
            }

        }
        tokenSource.Cancel();
    }
});
await Task.WhenAll(readTask);