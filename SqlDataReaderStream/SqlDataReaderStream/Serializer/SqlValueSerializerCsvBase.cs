using System.Collections.Generic;
using System.Text;

namespace SqlDataReaderStream.Serializer
{
    public class SqlValueSerializerCsvBase : ISqlValueSerializerReadValues
    {
        public const char ColumnSplitter = '\t';
        public const char RowSplitter = '\n';
        protected readonly Encoding Encoding;

        private string _LastRowMayBeOnlyFirstFragment;

        public SqlValueSerializerCsvBase(Encoding p_Encoding)
        {
            Encoding = p_Encoding;
        }

        public SqlValueSerializerCsvBase() : this(Encoding.UTF8)
        {
        }

        /// <summary>
        ///     Split BufferString to rows and columns and cache unprocessed last row (may be only first fragment of row without
        ///     all columns)
        /// </summary>
        /// <param name="p_ReadedBufferString"></param>
        /// <returns></returns>
        public IEnumerable<IEnumerable<string>> ReadValues(string p_ReadedBufferString, out string p_LastRowMayBeOnlyFirstFragment)
        {
            p_LastRowMayBeOnlyFirstFragment = null;
            List<List<string>> rowsValues = new List<List<string>>();
            string[] rows = p_ReadedBufferString.Split(RowSplitter);
            for (var iRow = 0; iRow < rows.Length - 1; iRow++)
            {
                string[] columns = rows[iRow].Split(ColumnSplitter);
                List<string> rowData = new List<string>(columns.Length);
                for (var iColumn = 0; iColumn < columns.Length; iColumn++)
                    rowData[iColumn] = columns[iColumn];
                rowsValues.Add(rowData);
            }
            p_LastRowMayBeOnlyFirstFragment = rows[rows.Length - 1];
            return rowsValues;
        }
    }
}