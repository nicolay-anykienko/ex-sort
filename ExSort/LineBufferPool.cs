using System;
using Microsoft.Extensions.ObjectPool;

namespace ExSort
{
    public class LineBufferPool : DefaultObjectPool<LineBuffer>
    {
        public LineBufferPool(Func<LineBuffer> objectFactory, int maximumRetained) : base(new DefaultPolicy(objectFactory), maximumRetained)
        {
        }
        
        private class DefaultPolicy : PooledObjectPolicy<LineBuffer>
        {
            private readonly Func<LineBuffer> factory;

            public DefaultPolicy(Func<LineBuffer> factory)
            {
                this.factory = factory;
            }

            public override LineBuffer Create()
            {
                return factory();
            }

            public override bool Return(LineBuffer obj)
            {
                return true;
            }
        }
    }
}