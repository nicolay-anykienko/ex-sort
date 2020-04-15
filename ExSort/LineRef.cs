using System;

namespace ExSort
{
    public struct LineRef : IComparable, IComparable<LineRef>
    {
        public int LineIndex { get; }

        public LineBuffer LineBuffer { get; }

        public LineRef(LineBuffer lineBuffer, int lineIndex)
        {
            this.LineIndex = lineIndex;
            this.LineBuffer = lineBuffer;
        }

        public override bool Equals(object obj)
        {
            return (obj is LineRef other)
                   && LineIndex == other.LineIndex
                   && LineBuffer == other.LineBuffer;
        }

        public int CompareTo(object obj)
        {
            return CompareTo((LineRef) obj);
        }

        public int CompareTo(LineRef other)
        {
            return LineComparer.Default.Compare(this, other);
        }

        public override string ToString()
        {
            int start = LineBuffer.GetLineStart(LineIndex);
            int end = LineBuffer.GetLineEnd(LineIndex);
            return new string(LineBuffer.InternalBuffer, start, end - start);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                hash = hash * 31 + LineIndex;
                hash = hash * 31 + LineBuffer.GetHashCode();
                return hash;
            }
        }
    }
}