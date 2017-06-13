using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlDataReaderStream.Serializer
{
    public class SqlValueSerializerCsvBase
    {
        public const char ColumnSplitter = '\t';
        public const char RowSplitter = '\n';
        protected readonly Encoding Encoding;

        private readonly List<Tuple<char, string>> _ReplaceSpecialChars = new List<Tuple<char, string>>
        {
            new Tuple<char, string>('\t', "\\t"),
            new Tuple<char, string>('\n', "\\n"),
            new Tuple<char, string>('\r', "\\r")
        };

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
        /// <param name="p_ColumnTypes"></param>
        /// <param name="p_LastRowMayBeOnlyFirstFragment"></param>
        /// <returns></returns>
        public IEnumerable<object[]> ReadValues(string p_ReadedBufferString, List<TypeWithConverter> p_ColumnTypes, out string p_LastRowMayBeOnlyFirstFragment)
        {
            List<object[]> rowsValues = new List<object[]>();
            string[] rows = p_ReadedBufferString.Split(RowSplitter);
            for (var iRow = 0; iRow < rows.Length - 1; iRow++)
            {
                if (string.IsNullOrEmpty(rows[iRow])) continue;
                List<string> rowDataStrings = rows[iRow].Split(ColumnSplitter).ToList();
                if (rowDataStrings.Count != p_ColumnTypes.Count)
                    throw new Exception($"Row contains {rowDataStrings.Count} columns in stream data but DataTable schema contains {p_ColumnTypes.Count} columns");
                object[] rowDataObjects = new object[rowDataStrings.Count];
                for (var iColumn = 0; iColumn < rowDataStrings.Count; iColumn++)
                {
                    var columnString = rowDataStrings[iColumn];
                    var columntype = p_ColumnTypes[iColumn];
                    rowDataObjects[iColumn] = ConvertColumnData(columntype, columnString);
                }
                rowsValues.Add(rowDataObjects);
            }
            p_LastRowMayBeOnlyFirstFragment = rows[rows.Length - 1];
            return rowsValues;
        }

        protected virtual object ConvertColumnData(TypeWithConverter p_ColumnType, string p_ColumnData)
        {
            if (p_ColumnType.Type == typeof(string))
            {
                _ReplaceSpecialChars.ForEach(m =>
                {
                    if (p_ColumnData.Contains(m.Item2))
                        p_ColumnData = p_ColumnData.Replace(m.Item2, m.Item1.ToString());
                });
                return p_ColumnData;
            }
            return string.IsNullOrEmpty(p_ColumnData) ? null : p_ColumnType.TypeConverter.ConvertFromString(p_ColumnData);
            //Convert.ChangeType not suppor GUID
            //return string.IsNullOrEmpty(p_ColumnData) ? null : Convert.ChangeType(p_ColumnData, p_ColumnType);
        }

        protected void BeforeWriteStreamCheckAndReplaceSpecialChars(ref object p_Value)
        {
            foreach (Tuple<char, string> m in _ReplaceSpecialChars)
                if (p_Value.ToString().Contains(m.Item1))
                    p_Value = p_Value.ToString().Replace(m.Item1.ToString(), m.Item2);
        }
    }
}