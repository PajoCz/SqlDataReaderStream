using System;
using System.Collections.Generic;
using System.IO;

namespace SqlDataReaderStream.Serializer
{
    public interface ISqlValueSerializer
    {
        void WriteObject(Stream p_Stream, object p_Value, Type p_TableColumnDataType, bool p_LastValueOfRow);
        IEnumerable<object[]> ReadValues(string p_ReadedBufferString, List<Type> p_ColumnTypes, out string p_LastRowMayBeOnlyFirstFragment);
    }
}