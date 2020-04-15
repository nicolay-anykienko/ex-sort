using System;
using System.Collections.Generic;
using System.IO;

namespace ExSort
{
    public class LineBuffer
    {
        private int charCount = 0;
        private readonly List<int> lineBreaks;

        public char[] InternalBuffer { get; }
        
        public int LineCount => lineBreaks.Count;

        public LineBuffer(int capacity)
        {
            InternalBuffer = new char[capacity];
            lineBreaks = new List<int>(capacity / 50);
        }

        private unsafe void ComputeLineBreaks(int bufferLength)
        {
            lineBreaks.Clear();
            fixed (char* p = InternalBuffer)
            {
                for (int i = 0; i < bufferLength; ++i)
                {
                    if (p[i] == '\n')
                    {
                        lineBreaks.Add(i);
                    }
                }
            }
        }

        public LineRef this[int index] => new LineRef(this, index);

        public void WriteLine(StreamWriter writer, int lineIndex)
        {
            int start = GetLineStart(lineIndex);
            int length = GetLineEnd(lineIndex) - start;
            writer.Write(InternalBuffer, start, length);
            writer.Write('\n');
        }

        public int GetLineStart(int lineIndex)
        {
            return lineIndex == 0 ? 0 : lineBreaks[lineIndex - 1] + 1;
        }

        public int GetLineEnd(int lineIndex)
        {
            return lineBreaks[lineIndex];
        }

        public class Reader
        {
            private readonly StreamReader reader;
            private char[] incompleteLineData;

            public Reader(StreamReader reader)
            {
                this.reader = reader;
            }

            public StreamReader BaseReader => reader;

            public bool Read(LineBuffer target)
            {
                int readCharCount = 0;
                int incompleteLineCharCount = 0;

                if (incompleteLineData != null && incompleteLineData.Length > 0)
                {
                    Buffer.BlockCopy(incompleteLineData, 0, target.InternalBuffer, 0, sizeof(char) * incompleteLineData.Length);
                    readCharCount = incompleteLineData.Length;
                    incompleteLineData = null;
                }

                readCharCount += reader.Read(target.InternalBuffer, readCharCount, target.InternalBuffer.Length - readCharCount);

                target.ComputeLineBreaks(readCharCount);

                if (target.LineCount == 0 && !reader.EndOfStream)
                {
                    throw new InvalidOperationException("Line length exceeds buffer size.");
                }

                if (readCharCount > 0 && target.InternalBuffer[readCharCount - 1] != '\n')
                {
                    int lastLineBreakPlus1 = target.lineBreaks[target.lineBreaks.Count - 1] + 1;
                    incompleteLineCharCount = readCharCount - lastLineBreakPlus1;
                    incompleteLineData = new char[incompleteLineCharCount];
                    Buffer.BlockCopy(target.InternalBuffer, sizeof(char) * lastLineBreakPlus1, incompleteLineData, 0, sizeof(char) * incompleteLineCharCount);
                }

                target.charCount = readCharCount - incompleteLineCharCount;

                return target.charCount > 0;
            }
        }
    }
}