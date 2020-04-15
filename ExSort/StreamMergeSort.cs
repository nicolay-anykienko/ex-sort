using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ExSort
{
    public class StreamMergeSort<T>
    {
        private const int MAX_RECURSION_LEVEL = 30;
        
        private readonly IComparer<T> comparer;
        private readonly int degreeOfParallelism;
        private readonly int parallelStreamBufferSize;

        public StreamMergeSort(IComparer<T> comparer, int degreeOfParallelism = 2, int parallelStreamBufferSize = 1000)
        {
            this.comparer = comparer;
            this.degreeOfParallelism = degreeOfParallelism;
            this.parallelStreamBufferSize = parallelStreamBufferSize;
        }
        
        public IEnumerable<T> MergeSort(IEnumerable<T>[] streams)
        {
            return MergeSort(streams, new ResourceAllocator(degreeOfParallelism), 0);
        }
        
        private IEnumerable<T> MergeSort(IEnumerable<T>[] streams, ResourceAllocator threadsAllocator, int recursionLevel)
        {
            if (streams.Length > 2)
            {
                if (recursionLevel >= MAX_RECURSION_LEVEL)
                {
                    Console.WriteLine("MergeSort: max recursion level is reached. Applying heap merge sort");
                    
                    var result = HeapMergeSort(streams);
                    foreach (var value in result)
                    {
                        yield return value;
                    }
                }
                else
                {
                    var part1 = new IEnumerable<T>[streams.Length / 2];
                    var part2 = new IEnumerable<T>[streams.Length - part1.Length];
                    Array.Copy(streams, 0, part1, 0, part1.Length);
                    Array.Copy(streams, part1.Length, part2, 0, part2.Length);

                    var part1ExecuteInParallel = threadsAllocator.Allocate();
                    var part2ExecuteInParallel = threadsAllocator.Allocate();
                
                    var part1Result = part1ExecuteInParallel
                        ? MergeSortParallel(part1, threadsAllocator, recursionLevel + 1)
                        : MergeSort(part1, threadsAllocator, recursionLevel + 1);
                
                    var part2Result = part2ExecuteInParallel
                        ? MergeSortParallel(part2, threadsAllocator, recursionLevel + 1)
                        : MergeSort(part2, threadsAllocator, recursionLevel + 1);

                    var result = MergeSort(new[] { part1Result, part2Result }, threadsAllocator, recursionLevel + 1);
                    foreach (var value in result)
                    {
                        yield return value;
                    }
                }
            }
            else if (streams.Length == 2)
            {
                using var enumerator1 = streams[0].GetEnumerator();
                using var enumerator2 = streams[1].GetEnumerator();

                bool enumerator1HasData = enumerator1.MoveNext();
                bool enumerator2HasData = enumerator2.MoveNext();

                while (enumerator1HasData && enumerator2HasData)
                {
                    var value1 = enumerator1.Current;
                    var value2 = enumerator2.Current;

                    var compareResult = this.comparer.Compare(value1, value2);
                    if (compareResult < 0)
                    {
                        yield return value1;
                        enumerator1HasData = enumerator1.MoveNext();
                    }
                    else if (compareResult > 0)
                    {
                        yield return value2;
                        enumerator2HasData = enumerator2.MoveNext();
                    }
                    else
                    {
                        yield return value1;
                        yield return value2;
                        enumerator1HasData = enumerator1.MoveNext();
                        enumerator2HasData = enumerator2.MoveNext();
                    }
                }

                while (enumerator1HasData)
                {
                    yield return enumerator1.Current;
                    enumerator1HasData = enumerator1.MoveNext();
                }

                while (enumerator2HasData)
                {
                    yield return enumerator2.Current;
                    enumerator2HasData = enumerator2.MoveNext();
                }
            }
            else if (streams.Length == 1)
            {
                var stream = streams[0];
                foreach (var line in stream)
                {
                    yield return line;
                }
            }
        }

        private IEnumerable<T> MergeSortParallel(IEnumerable<T>[] streams, ResourceAllocator threadsAllocator, int recursionLevel)
        {
            var blockingCollection = new BlockingCollection<T>(parallelStreamBufferSize);

            Task.Factory.StartNew(() =>
                {
                    var result = MergeSort(streams, threadsAllocator, recursionLevel);
                    foreach (var line in result)
                    {
                        blockingCollection.Add(line);
                    }

                    blockingCollection.CompleteAdding();
                    threadsAllocator.Deallocate();
                },
                TaskCreationOptions.LongRunning
            );

            return blockingCollection.GetConsumingEnumerable();
        }
        
        private IEnumerable<T> HeapMergeSort(IEnumerable<T>[] streams)
        {
            if (streams.Length > 1)
            {
                IEnumerator<T>[] enumerators = new IEnumerator<T>[streams.Length];
                for (int i = 0; i < streams.Length; i++)
                {
                    enumerators[i] = streams[i].GetEnumerator();
                }
                
                var mergeSortMinHeap = new SortedLookup<T, int>(this.comparer);

                for (int i = 0; i < enumerators.Length; ++i)
                {
                    var enumerator = enumerators[i];
                    if (enumerator.MoveNext())
                    {
                        mergeSortMinHeap.Put(enumerator.Current, i);
                    }
                }

                while (mergeSortMinHeap.Count != 0)
                {
                    var kvp = mergeSortMinHeap.First();
                    var line = kvp.Key;
                    var streamRefs = kvp.Value;

                    mergeSortMinHeap.Remove(line);
                    foreach (var streamRef in streamRefs)
                    {
                        yield return line;
                    }

                    foreach (var streamRef in streamRefs)
                    {
                        var enumerator = enumerators[streamRef];
                        if (enumerator.MoveNext())
                        {
                            mergeSortMinHeap.Put(enumerator.Current, streamRef);
                        }
                    }
                }
            }
            else if (streams.Length == 1)
            {
                var stream = streams[0];
                foreach (var line in stream)
                {
                    yield return line;
                }
            }
        }
        
        private sealed class ResourceAllocator
        {
            private readonly SemaphoreSlim semaphore;

            public ResourceAllocator(int availableResources)
            {
                semaphore = new SemaphoreSlim(availableResources);
            }

            public bool Allocate()
            {
                return semaphore.Wait(0);
            }

            public void Deallocate()
            {
                semaphore.Release();
            }
        }
    }
}