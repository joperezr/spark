// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.Sql;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.Examples.Sql.Streaming
{
    /// <summary>
    /// The example is taken/modified from
    /// spark/examples/src/main/python/sql/streaming/structured_network_wordcount.py
    ///
    /// You can set up the data source as follow in a separated terminal:
    /// `$ nc -lk 9999`
    /// to start writing standard input to port 9999.
    /// </summary>
    internal sealed class StructuredNetworkWordCount : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 2)
            {
                Console.Error.WriteLine(
                    "Usage: StructuredNetworkWordCount <hostname> <port>");
                Environment.Exit(1);
            }

            string hostname = args[0];
            var port = int.Parse(args[1]);

            SparkSession spark = SparkSession
                .Builder()
                .AppName("StructuredNetworkWordCount")
                .GetOrCreate();

            var eventHubPosition = new EventHubPosition
            {
                Offset = "0",
                SeqNo = -1
            };

            var startingPositions = new Dictionary<string, EventHubPosition> {
                { JsonConvert.SerializeObject(new PostitionKey { EventHubName = "mytesteventhub", PartitionId = 0 }), eventHubPosition },
                { JsonConvert.SerializeObject(new PostitionKey { EventHubName = "mytesteventhub", PartitionId = 1 }), eventHubPosition },
            };

            DataFrame messages = spark
                .ReadStream()
                .Format("eventhubs")
                .Options(new Dictionary<string, string>
                {
                    {"eventhubs.connectionString", "" },
                    {"eventhubs.consumerGroup", "$Default" },
                    {"eventhubs.startingPositions", JsonConvert.SerializeObject(startingPositions) }
                }).Load()
                .SelectExpr(
                "CAST(Body AS String) AS word",
                "Partition",
                "Offset"
                );

            Func<Column, Column> charsInWord = Udf<string, string[]>(str =>
            {
                string[] array = new string[str.Length];
                for (int i = 0; i < str.Length; i++)
                    array[i] = str[i].ToString();
                return array;
            });
            DataFrame charCounts = messages.Select(Explode(charsInWord(messages["word"])).As("character")).GroupBy("character").Count().OrderBy(Desc("count"));


            Spark.Sql.Streaming.StreamingQuery query = charCounts
                .WriteStream()
                .QueryName("aggregates")
                .OutputMode("complete")
                .Format("memory")
                .Start();

            Task.Run(() =>
            {
                while (true)
                {
                    Console.WriteLine("Top 100 rows:");
                    spark.Sql("select * from aggregates").Show(100);

                    Thread.Sleep(10_000);
                }
            });

            query.AwaitTermination();
        }
    }

    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public class EventHubPosition
    {
        public string Offset { get; set; }

        public long SeqNo { get; set; }

        public string EnqueuedTime { get; set; }

        public bool IsInclusive { get; set; }
    }

    /// <summary>
    /// EventHub postition key definition by partition id.
    /// </summary>
    public class PostitionKey
    {
        [JsonProperty("ehName")]
        public string EventHubName { get; set; }

        [JsonProperty("partitionId")]
        public int PartitionId { get; set; }
    }
}
