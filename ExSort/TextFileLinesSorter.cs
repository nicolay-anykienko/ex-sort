using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.ObjectPool;
using Microsoft.VisualBasic.Devices;

namespace ExSort
{
    public class TextFileLinesSorter
    {
        private readonly string inputFile;
        private readonly string outputFile;
        private readonly string tempFilesTemplate;
        private readonly long? memoryLimit;
        private readonly int degreeOfParallelism;
        private readonly ILineComparer lineComparer;

        public TextFileLinesSorter(
            string inputFile,
            string outputFile,
            string tempFilesTemplate,
            ILineComparer lineComparer = null,
            long? memoryLimit = null,
            int? degreeOfParallelism = null)
        {
            this.inputFile = inputFile;
            this.outputFile = outputFile;
            this.tempFilesTemplate = tempFilesTemplate;
            this.memoryLimit = memoryLimit;
            this.degreeOfParallelism = degreeOfParallelism ?? Environment.ProcessorCount;
            this.lineComparer = lineComparer ?? LineComparer.Default;
        }

        public void Sort()
        {
            CreateDirectories();

            var sw = Stopwatch.StartNew();
            SortChunks();
            MemoryMaintenance(true);
            sw.Stop();
            Console.WriteLine($"Sorting chunks time: {sw.Elapsed.TotalSeconds} s.");

            sw.Restart();            
            MergeSortChunks();
            sw.Stop();
            Console.WriteLine($"Merging chunks time: {sw.Elapsed.TotalSeconds} s.");

            DeleteTempFiles();
        }

        private void CreateDirectories()
        {
            string tempDirectory = Path.GetDirectoryName(tempFilesTemplate);
            if (!Directory.Exists(tempDirectory))
            {
                Directory.CreateDirectory(tempDirectory);
            }

            string outputDirectory = Path.GetDirectoryName(outputFile);
            if (!Directory.Exists(outputDirectory))
            {
                Directory.CreateDirectory(outputDirectory);
            }
        }

        private void DeleteTempFiles()
        {
            string directory = Path.GetDirectoryName(tempFilesTemplate);
            string fileName = Path.GetFileName(tempFilesTemplate);
            string[] files = Directory.GetFiles(directory, string.Format(fileName, "*"));

            foreach (var file in files)
            {
                File.Delete(file);
            }
        }

        private void SortChunks()
        {
            Console.WriteLine("Sorting chunks...");

            using var inputFileStream = File.Open(inputFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            var chunkSize = CalculateChunkSize(inputFileStream.Length);
            var chunkPool = new LineBufferPool(() => new LineBuffer((int) chunkSize / sizeof(char)), degreeOfParallelism);
            var chunks = ReadAsChunks(inputFileStream, chunkPool);
            
            long bytesProcessed = 0;
            long bytesTotal = inputFileStream.Length;

            var partitioner = Partitioner.Create(chunks, EnumerablePartitionerOptions.NoBuffering);
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = degreeOfParallelism };
            Parallel.ForEach(
                partitioner,
                parallelOptions,
                (item, state, chunkIndex) =>
                {
                    var (chunk, bytesRead) = item;
                    SortChunk(chunk, chunkIndex + 1);
                    Interlocked.Add(ref bytesProcessed, bytesRead);
                    chunkPool.Return(chunk);
                    MemoryMaintenance();
                    Console.WriteLine($"Processed batch {chunkIndex + 1} of size {bytesRead} with {chunk.LineCount} lines. Total progress: {bytesProcessed}/{bytesTotal} ({Math.Truncate(100.0 * bytesProcessed / bytesTotal)} %)");
                }
            );
        }

        private void MemoryMaintenance(bool force = false)
        {
            if (force || Process.GetCurrentProcess().WorkingSet64 > memoryLimit + 100 * 1024 * 1024)
            {
                Console.WriteLine("GC collect, LOH compact");
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect();
            }
        }

        private void SortChunk(LineBuffer chunk, long chunkNumber)
        {
            var lines = new int[chunk.LineCount];
            for (int i = 0; i < lines.Length; i++)
            {
                lines[i] = i;
            }

            var sw = Stopwatch.StartNew();
            Array.Sort(lines, (x, y) => this.lineComparer.Compare(new LineRef(chunk, x), new LineRef(chunk, y)));
            sw.Stop();
            Console.WriteLine($"Array.Sort time: {sw.Elapsed.TotalSeconds} s.");
            
            using StreamWriter chunkWriter = File.CreateText(string.Format(tempFilesTemplate, chunkNumber));

            foreach (var lineIndex in lines)
            {
                chunk.WriteLine(chunkWriter, lineIndex);
            }
        }

        private void MergeSortChunks()
        {
            string directory = Path.GetDirectoryName(tempFilesTemplate);
            string fileName = Path.GetFileName(tempFilesTemplate);
            string[] files = Directory.GetFiles(directory, string.Format(fileName, "*"));
            
            Console.WriteLine($"Merge sort {files.Length} chunks...");

            if (files.Length == 0)
            {
                return;
            }

            if (files.Length == 1)
            {
                File.Move(files[0], outputFile);
                return;
            }

            const int fileReaderBufferSize = 1024 * 1024;
                        
            var lineReaders = new LineReader[files.Length];

            long bytesTotal = 0;
            for (int i = 0; i < files.Length; ++i)
            {
                bytesTotal += new FileInfo(files[i]).Length;
                lineReaders[i] = new LineReader(new StreamReader(files[i])); 
            }

            var lineStreams = lineReaders.Select(reader => reader.AsEnumerable()).ToArray();
            var mergeSort = new StreamMergeSort<string>(this.lineComparer, degreeOfParallelism: Math.Min(2, degreeOfParallelism));
            var sortedStream = mergeSort.MergeSort(lineStreams);

            long processedLines = 0;
            var outputFileStream = new FileStream(this.outputFile, FileMode.Create);
            var bufferedStream = new BufferedStream(outputFileStream, 1024 * 1024);
            var outputWriter = new StreamWriter(bufferedStream);
            foreach (var line in sortedStream)
            {
                outputWriter.Write(line);
                outputWriter.Write("\r\n"); 
                processedLines++;
                
                if (processedLines % 1000000 == 0)
                {
                    Console.WriteLine($"Processed lines {processedLines}. Total progress: {bufferedStream.Position}/{bytesTotal} ({Math.Truncate(100.0 * bufferedStream.Position / bytesTotal)} %)");
                }
            }

            outputWriter.Flush();
            Console.WriteLine($"Processed lines {processedLines}. Total progress: {bufferedStream.Position}/{bytesTotal} ({Math.Truncate(100.0 * bufferedStream.Position / bytesTotal)} %)");

            outputWriter.Dispose();
            foreach (var reader in lineReaders)
            {
                reader.Dispose();
            }
        }

        static IEnumerable<(LineBuffer chunk, long bytesRead)> ReadAsChunks(Stream stream, ObjectPool<LineBuffer> chunkPool)
        {
            using var streamReader = new StreamReader(stream);
            var chunkReader = new LineBuffer.Reader(streamReader);
            var streamLock = new object();

            while (true)
            {
                LineBuffer chunk = chunkPool.Get();
                long positionBefore;
                long positionAfter;
                lock (streamLock)
                {
                    positionBefore = stream.Position;
                    if (!chunkReader.Read(chunk))
                    {
                        yield break;
                    }
                    positionAfter = stream.Position;
                }

                yield return (chunk, positionAfter - positionBefore);
            }
        }

        private long CalculateChunkSize(long textFileSize)
        {
            long availableMemory = (long) (new ComputerInfo().AvailablePhysicalMemory * 0.8);
            long approximateFileSizeInMemory = textFileSize * sizeof(char);

            long suggestedMemoryLimit = approximateFileSizeInMemory < availableMemory
                ? approximateFileSizeInMemory
                : availableMemory;

            long resultMemoryLimit = Math.Min(suggestedMemoryLimit, memoryLimit ?? long.MaxValue);
            long chunkSize = resultMemoryLimit / degreeOfParallelism;

            return chunkSize;
        }
    }
}