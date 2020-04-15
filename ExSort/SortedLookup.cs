using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace ExSort
{
    public class SortedLookup<TKey, TValue>
    {
        private readonly SortedDictionary<TKey, Values<TValue>> sortedDictionary;

        public int Count => this.sortedDictionary.Count;

        public SortedLookup(IComparer<TKey> keyComparer)
        {
            this.sortedDictionary = new SortedDictionary<TKey, Values<TValue>>(keyComparer);
        }
        
        public void Put(TKey key, TValue value)
        {
            if (sortedDictionary.TryGetValue(key, out Values<TValue> valueStorage))
            {
                valueStorage.Add(value);
            }
            else
            {
                sortedDictionary.Add(key, new Values<TValue>(value));
            }
        }

        public void Remove(TKey key)
        {
            sortedDictionary.Remove(key);
        }

        public KeyValuePair<TKey, Values<TValue>> First()
        {
            return sortedDictionary.First();
        }
        
        public class Values<T> : IEnumerable<T>
        {
            private readonly T firstValue;
        
            private List<T> nextValues;

            public Values(T value)
            {
                this.firstValue = value;
            }

            public void Add(T value)
            {
                if (nextValues == null)
                {
                    nextValues = new List<T>(4);
                }

                nextValues.Add(value);
            }

            public IEnumerator<T> GetEnumerator()
            {
                yield return firstValue;

                if (nextValues != null)
                {
                    foreach (var value in nextValues)
                    {
                        yield return value;
                    }
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}