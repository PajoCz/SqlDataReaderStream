using System.Collections.Generic;

namespace SqlDataReaderStream.Serializer
{
    public interface ISqlValueSerializerReadValues
    {
        IEnumerable<IEnumerable<string>> ReadValues(string p_ReadedBufferString, out string p_LastRowMayBeOnlyFirstFragment);
    }
}