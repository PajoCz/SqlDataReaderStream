using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream
{
    public class StreamToEnumerableDataRows : IEnumerable<DataRow>
    {
        private readonly int _BufferSize;
        private readonly DataTable _DataTable;
        private readonly ISqlValueSerializer _Serializer;
        private readonly Stream _Stream;

        public StreamToEnumerableDataRows(Stream p_Stream, DataTable p_DataTable, ISqlValueSerializer p_Serializer, int p_BufferSize = StreamToDataTableRows.DefaultCopyBufferSize)
        {
            _Stream = p_Stream;
            _DataTable = p_DataTable;
            _Serializer = p_Serializer;
            _BufferSize = p_BufferSize;
        }

        public IEnumerator<DataRow> GetEnumerator()
        {
            return new StreamToEnumaratorDataRows(_Stream, _DataTable, _Serializer, _BufferSize);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}