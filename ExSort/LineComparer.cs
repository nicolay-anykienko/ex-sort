using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace ExSort
{
    public class LineComparer : ILineComparer
    {
        public static LineComparer Default { get; } = new LineComparer();
        
        public unsafe int Compare(LineRef x, LineRef y)
        {
            if (x.LineIndex == y.LineIndex && x.LineBuffer == y.LineBuffer)
            {
                return 0;
            }

            int offset1 = x.LineBuffer.GetLineStart(x.LineIndex);
            int length1 = x.LineBuffer.GetLineEnd(x.LineIndex) - offset1;
            int offset2 = y.LineBuffer.GetLineStart(y.LineIndex);
            int length2 = y.LineBuffer.GetLineEnd(y.LineIndex) - offset2;

            fixed (char* line1 = &x.LineBuffer.InternalBuffer[offset1])
            fixed (char* line2 = &y.LineBuffer.InternalBuffer[offset2])
            {
                return CompareInternal(line1, length1, line2, length2);
            }
        }

        public unsafe int Compare(string x, string y)
        {
            fixed (char* line1 = x)
            fixed (char* line2 = y)
            {
                return CompareInternal(line1, x.Length, line2, y.Length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int CompareInternal(char* line1, int length1, char* line2, int length2)
        {
            char* str1Offset = GetStringStart(line1, length1);
            char* str2Offset = GetStringStart(line2, length2);
            int str1Len = (int) ((long) line1 - (long) str1Offset) / sizeof(char) + length1;
            int str2Len = (int) ((long) line2 - (long) str2Offset) / sizeof(char) + length2;
            int minLen = Math.Min(str1Len, str2Len);
            char* p1 = str1Offset;
            char* p2 = str2Offset;

            while (minLen != 0)
            {
                char c1 = *p1;
                char c2 = *p2;

                if (c1 > c2)
                {
                    return 1;
                }

                if (c1 < c2)
                {
                    return -1;
                }

                ++p1;
                ++p2;
                --minLen;
            }

            if (str1Len == str2Len)
            {
                long l1Num = GetNumber(line1, str1Offset);
                long l2Num = GetNumber(line2, str2Offset);
                if (l1Num > l2Num)
                {
                    return 1;
                }

                if (l1Num < l2Num)
                {
                    return -1;
                }

                return 0;
            }

            if (str1Len > str2Len)
            {
                return 1;
            }

            if (str1Len < str2Len)
            {
                return -1;
            }

            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe char* GetStringStart(char* p, int len)
        {
            char* eop = p + len;
            while (p < eop)
            {
                char c = *p;
                if (c <= '9' && c >= '0')
                {
                    ++p;
                }
                else
                {
                    return p;
                }
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe long GetNumber(char* p, char* pEnd)
        {
            int result = 0;
            while (p < pEnd)
            {
                result = result * 10 + (*p++) - '0';
            }

            return result;
        }
    }
}