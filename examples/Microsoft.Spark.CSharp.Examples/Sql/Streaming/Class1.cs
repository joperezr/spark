// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.Examples.Sql
{
    /// <summary>
    /// A simple example demonstrating basic Spark SQL features.
    /// </summary>
    internal sealed class BasicTelemetry : IExample
    {
        public void Run(string[] args)
        {
            if (args.Length != 1)
            {
                Console.Error.WriteLine(
                    "Usage: Basic <path to SPARK_HOME/examples/src/main/resources/people.json>");
                Environment.Exit(1);
            }

            SparkSession spark = SparkSession
                .Builder()
                .AppName(".NET Spark SQL basic example")
                .Config("spark.some.config.option", "some-value")
                .GetOrCreate();

            DataFrame geoData = spark
                .Read()
                .Option("delimiter", "\t")
                .Option("header", "true")
                .Csv(@"C:\Users\joperezr\Desktop\ReverseIpLookup.csv")
                .Repartition(40);

            Func<Column, Column, Column> MergeTwoColumns = Udf<int?, int?, string>((keyOctect, maxThirdOctect) => JsonSerializer.Serialize(new[] { keyOctect.Value, maxThirdOctect.Value }));

            Func<Column, Column, Column, Column> MergeThreeColumns = Udf<int?, string, string, string>((minThirdOctet, countryIsoCode, cityName) => JsonSerializer.Serialize(new[] { minThirdOctet.Value.ToString(), countryIsoCode, cityName }));

            Func<Column, Column> CreateDictionaryKey = Udf<string, int>((str) => JsonSerializer.Deserialize<int[]>(str)[0]);
            Func<Column, Column, Column> CreateDictionaryValue = Udf<string, string, string>((key, value) => JsonSerializer.Serialize(new[] { JsonSerializer.Deserialize<int[]>(key)[1].ToString(), value }));

            var geoLookupTemp = geoData
                .SelectExpr("(CAST(FirstOctet AS INT) * 256 + CAST(SecondOctet AS INT)) AS KeyOctet",
                            "CAST(MinThirdOctet AS INT) AS MinThirdOctet",
                            "CAST(MaxThirdOctet AS INT) AS MaxThirdOctet",
                            "CountryIsoCode",
                            "CityName")
                .SelectExpr("KeyOctet",
                            "IF(MaxThirdOctet>=MinThirdOctet, MaxThirdOctet, MinThirdOctet) AS MaxThirdOctet",
                            "IF(MinThirdOctet<=MaxThirdOctet, MinThirdOctet, MaxThirdOctet) AS MinThirdOctet",
                            "CountryIsoCode", "CityName");

            var geoLookup = geoLookupTemp
                .Select(MergeTwoColumns(geoLookupTemp["KeyOctet"], geoLookupTemp["MaxThirdOctet"]).As("_1"), MergeThreeColumns(geoLookupTemp["MinThirdOctet"], geoLookupTemp["CountryIsoCode"], geoLookupTemp["CityName"]).As("_2"));

            DataFrame groupedGeoLookup = geoLookup
                .GroupBy(geoLookup["_1"])
                .Apply(
                    new StructType(new[]
                    {
                        new StructField("_1", new StringType()),
                        new StructField("_2", new StringType())
                    }),
                    r => KeepMinimumValueForKey(r, "_1", "_2"));

            DataFrame TreeMap = groupedGeoLookup
                .Select(CreateDictionaryKey(groupedGeoLookup["_1"]).As("_1"), CreateDictionaryValue(groupedGeoLookup["_1"], groupedGeoLookup["_2"]).As("_2"));

            DataFrame reducedTreeMap = TreeMap
                .GroupBy(TreeMap["_1"])
                .Apply(
                    new StructType(new[]
                    {
                        new StructField("_1", new IntegerType()),
                        new StructField("_2", new StringType())
                    }),
                    r => ReduceByKeyDictionary(r, "_1", "_2"));

            reducedTreeMap.PrintSchema();
            reducedTreeMap.Show(50, 50);

            spark.Stop();
        }

        private static Apache.Arrow.RecordBatch ReduceByKeyDictionary(
            Apache.Arrow.RecordBatch records,
            string groupFiledName,
            string stringFieldName)
        {
            int stringFieldIndex = records.Schema.GetFieldIndex(stringFieldName);
            Apache.Arrow.StringArray stringValues = records.Column(stringFieldIndex) as Apache.Arrow.StringArray;

            List<string[]> result = new List<string[]>();
            for (int i = 0; i < stringValues.Length; ++i)
            {
                string current = stringValues.GetString(i);
                string[] strArr = JsonSerializer.Deserialize<string[]>(current);
                result.Add(strArr);
            }

            var x = JsonSerializer.Serialize(result.ToArray());
            int groupFieldIndex = records.Schema.GetFieldIndex(groupFiledName);
            Apache.Arrow.Field groupField = records.Schema.GetFieldByIndex(groupFieldIndex);


            return new Apache.Arrow.RecordBatch(
                new Apache.Arrow.Schema.Builder()
                    .Field(groupField)
                    .Field(f => f.Name(stringFieldName).DataType(Apache.Arrow.Types.StringType.Default))
                    .Build(),
                new Apache.Arrow.IArrowArray[]
                {
                    records.Column(groupFieldIndex),
                    new Apache.Arrow.StringArray.Builder().Append(x).Build()
                },
            1);
        }

        private static Apache.Arrow.RecordBatch KeepMinimumValueForKey(
            Apache.Arrow.RecordBatch records,
            string groupFiledName,
            string stringFieldName)
        {
            int stringFieldIndex = records.Schema.GetFieldIndex(stringFieldName);
            Apache.Arrow.StringArray stringValues = records.Column(stringFieldIndex) as Apache.Arrow.StringArray;
            int min = int.MaxValue;
            string minString = string.Empty;
            
            for (int i = 0; i < stringValues.Length; ++i)
            {
                string current = stringValues.GetString(i);
                int currentValue = int.Parse(JsonSerializer.Deserialize<string[]>(current)[0]);
                if (currentValue <= min)
                {
                    min = currentValue;
                    minString = current;
                }
            }

            int groupFieldIndex = records.Schema.GetFieldIndex(groupFiledName);
            Apache.Arrow.Field groupField = records.Schema.GetFieldByIndex(groupFieldIndex);

            return new Apache.Arrow.RecordBatch(
                new Apache.Arrow.Schema.Builder()
                    .Field(groupField)
                    .Field(f => f.Name(stringFieldName).DataType(Apache.Arrow.Types.StringType.Default))
                    .Build(),
                new Apache.Arrow.IArrowArray[]
                {
                    records.Column(groupFieldIndex),
                    new Apache.Arrow.StringArray.Builder().Append(minString).Build()
                },
                1);
        }
    }
}
