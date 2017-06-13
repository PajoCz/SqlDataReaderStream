using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Text;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream
{
    /// <summary>
    ///     Client-side helper for reading data from Stream to DataTable with specific ISqlValueSerializer
    /// </summary>
    public class StreamToDataTableRows
    {
        //_DefaultCopyBufferSize - from MS implementation of Stream.CopyTo
        //We pick a value that is the largest multiple of 4096 that is still smaller than the large object heap threshold (85K).
        // The CopyTo/CopyToAsync buffer is short-lived and is likely to be collected at Gen0, and it offers a significant
        // improvement in Copy performance.
        internal const int DefaultCopyBufferSize = 81920;   //81920 = 80 * 1024 = 80kB

        private string _LastRowMayBeOnlyFirstFragment;

        public void ReadStreamToDataTable(Stream p_Stream, DataTable p_DataTable, ISqlValueSerializer p_Serializer, int p_BufferSize = DefaultCopyBufferSize)
        {
            byte[] buffer = new byte[p_BufferSize];
            var readed = int.MaxValue;
            _LastRowMayBeOnlyFirstFragment = string.Empty;
            List<TypeWithConverter> columnTypes = new List<TypeWithConverter>(p_DataTable.Columns.Count);
            foreach (DataColumn column in p_DataTable.Columns)
                columnTypes.Add(new TypeWithConverter(column.DataType, TypeDescriptor.GetConverter(column.DataType)));
            while (readed > 0)
            {
                readed = p_Stream.Read(buffer, 0, buffer.Length);
                var readedString = _LastRowMayBeOnlyFirstFragment + Encoding.UTF8.GetString(buffer, 0, readed);
                IEnumerable<object[]> readedSplitted = p_Serializer.ReadValues(readedString, columnTypes, out _LastRowMayBeOnlyFirstFragment);
                foreach (object[] rowData in readedSplitted)
                    p_DataTable.Rows.Add(rowData);
            }
        }
    }
}