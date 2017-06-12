using System;
using System.IO;

namespace SqlDataReaderStream.Serializer
{
    public class SqlValueSerializerCsvSimple : SqlValueSerializerCsvBase, ISqlValueSerializer
    {
        public readonly string ColumnSplitterString = ColumnSplitter.ToString();
        public readonly string RowSplitterString = RowSplitter.ToString();

        public void WriteObject(Stream p_Stream, object p_Value, Type p_TableColumnDataType, bool p_LastValueOfRow)
        {
            string valString;
            if (p_Value == null && p_TableColumnDataType == null && p_LastValueOfRow)
            {
                valString = RowSplitterString;
            }
            else
            {
                if (p_TableColumnDataType == typeof(string))
                    BeforeWriteStreamCheckAndReplaceSpecialChars(ref p_Value);
                valString = p_LastValueOfRow ? p_Value + RowSplitterString : p_Value + ColumnSplitterString;
            }
            byte[] bytes = Encoding.GetBytes(valString);
            p_Stream.Write(bytes, 0, bytes.Length);
        }
    }
}