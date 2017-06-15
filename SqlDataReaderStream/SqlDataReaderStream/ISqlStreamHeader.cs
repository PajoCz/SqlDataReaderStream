using System.Data;
using System.IO;

namespace SqlDataReaderStream
{
    public interface ISqlStreamHeader
    {
        void WriteToStream(Stream p_Stream, DataTable p_DataTableWithoutData);
        object ReadHeader(Stream p_Stream, int p_BufferSize = StreamToDataTableRows.DefaultCopyBufferSize);
    }
}