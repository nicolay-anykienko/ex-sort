using System.Collections.Generic;

namespace ExSort
{
    public interface ILineComparer : IComparer<LineRef>, IComparer<string>
    {
    }
}