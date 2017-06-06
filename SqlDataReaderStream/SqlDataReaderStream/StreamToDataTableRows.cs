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
        private string LastRowMayBeOnlyFirstFragment;

        public void ReadStreamToDataTable(Stream p_Stream, DataTable p_DataTable, ISqlValueSerializer p_Serializer, int p_BufferSize = 8 * 1024 * 10)
        {
            byte[] buffer = new byte[p_BufferSize];
            var readed = int.MaxValue;
            LastRowMayBeOnlyFirstFragment = string.Empty;
            List<TypeWithConverter> columnTypes = new List<TypeWithConverter>(p_DataTable.Columns.Count);
            foreach (DataColumn column in p_DataTable.Columns)
                columnTypes.Add(new TypeWithConverter(column.DataType, TypeDescriptor.GetConverter(column.DataType)));
            while (readed > 0)
            {
                readed = p_Stream.Read(buffer, 0, buffer.Length);
                var readedString = LastRowMayBeOnlyFirstFragment + Encoding.UTF8.GetString(buffer, 0, readed);
                IEnumerable<object[]> readedSplitted = p_Serializer.ReadValues(readedString, columnTypes, out LastRowMayBeOnlyFirstFragment);
                foreach (object[] rowData in readedSplitted)
                    p_DataTable.Rows.Add(rowData);
            }
        }
    }
}