using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
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

        public void ReadStreamToDataTable(Stream p_Stream, DataTable p_DataTable, ISqlValueSerializer p_Serializer, int p_BufferSize = 8 * 1024)
        {
            byte[] buffer = new byte[p_BufferSize];
            var readed = int.MaxValue;
            LastRowMayBeOnlyFirstFragment = string.Empty;
            while (readed > 0)
            {
                readed = p_Stream.Read(buffer, 0, buffer.Length);
                var readedString = LastRowMayBeOnlyFirstFragment + Encoding.UTF8.GetString(buffer, 0, readed);
                IEnumerable<IEnumerable<string>> readedSplitted = p_Serializer.ReadValues(readedString, out LastRowMayBeOnlyFirstFragment);
                CopyDataToDataTable(readedSplitted, p_DataTable);
            }
        }

        private static void CopyDataToDataTable(IEnumerable<IEnumerable<string>> p_ReadedSplitted, DataTable p_DataTable)
        {
            foreach (IEnumerable<string> row in p_ReadedSplitted)
            {
                IEnumerable<string> columnStrings = row as IList<string> ?? row.ToList();
                if (columnStrings.Count() != p_DataTable.Columns.Count)
                    throw new Exception($"Row contains {columnStrings.Count()} columns in stream data but DataTable schema contains {p_DataTable.Columns.Count} columns");
                object[] rowData = new object[columnStrings.Count()];
                var iColumn = 0;
                foreach (var columnString in columnStrings)
                {
                    var dataTableColumn = p_DataTable.Columns[iColumn];
                    rowData[iColumn] = string.IsNullOrEmpty(columnString) ? null : Convert.ChangeType(columnString, dataTableColumn.DataType);
                    iColumn++;
                }
                p_DataTable.Rows.Add(rowData);
            }
        }
    }
}