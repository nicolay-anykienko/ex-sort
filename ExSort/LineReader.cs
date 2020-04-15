using System;
using System.Collections.Generic;
using System.IO;

namespace ExSort
{
    public class LineReader : IDisposable
    {
        private readonly StreamReader streamReader;

        public LineReader(StreamReader streamReader)
        {
            this.streamReader = streamReader;
        }

        public IEnumerable<string> AsEnumerable()
        {
            while (!streamReader.EndOfStream)
            {
                yield return streamReader.ReadLine();
            }
        }

        public void Dispose()
        {
            streamReader.Dispose();
        }
    }
}