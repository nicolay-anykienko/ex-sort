using System;
using System.IO;
using System.Text;
using MMLib.RapidPrototyping.Generators;
using System.Diagnostics;

namespace ExSort
{
    class Program
    {
        private const string file = "bimba20.txt";
        
        private static readonly string INPUT_FILE_PATH = $"c:/DevTest/{file}";
        private static readonly string CHUNK_FILE_PATH = $"c:/DevTest/sort_chunks/{{0}}.{file}";
        private static readonly string SORTED_FILE_PATH = $"c:/DevTest/sorted.{file}";

        static void Main()
        {   
         //   CreateFile(2000);
            var sw = Stopwatch.StartNew();
            
            var sorter = new TextFileLinesSorter(
                INPUT_FILE_PATH, 
                SORTED_FILE_PATH, 
                CHUNK_FILE_PATH,
                memoryLimit: 1000L * 1024 * 1024,
                degreeOfParallelism: 4);
            
            sorter.Sort();
            
            sw.Stop();
            Console.WriteLine($"Total time: {sw.Elapsed.TotalSeconds} s.");
        }

        static void CreateFile(int mbSize)
        {
            int seed = 123; //Environment.TickCount;
            LoremIpsumGenerator g = new LoremIpsumGenerator(seed);
            Random r = new Random(seed);

            long bytesCount = 0;
            long linesCount = 0;
            long targetBytesCount = 1024L * 1024 * mbSize;

            using (StreamWriter writer = File.CreateText(INPUT_FILE_PATH))
            {
                bytesCount += writer.BaseStream.Length;
                while (bytesCount < targetBytesCount)
                {
                    string str = $"{r.Next()}. {g.Next(1, 5)}";
                    bytesCount += Encoding.UTF8.GetByteCount(str);
                    linesCount++;
                    writer.Write(str);
                    if (linesCount % 100000 == 0)
                    {
                        Console.WriteLine(
                            $"{bytesCount} of {targetBytesCount} bytes written ({100 * bytesCount / targetBytesCount}%)");
                    }
                }

                if (linesCount % 1000 != 0)
                {
                    Console.WriteLine(
                        $"{bytesCount} of {targetBytesCount} bytes written ({100 * bytesCount / targetBytesCount}%)");
                }
            }

            Console.WriteLine("CreateFile completed");
        }
    }
}