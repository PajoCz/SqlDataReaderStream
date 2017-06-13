//#define log

using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using SqlDataReaderStream.Serializer;
#if log
using System.Diagnostics;
#endif
#if log
using System.Text;
#endif

namespace SqlDataReaderStream
{
    internal class SqlStreamEngine
    {
        public const string OriginalColumnName = "OriginalColumnName";

        private readonly SqlDataReader _DataReader;
        private readonly ISqlValueSerializer _SqlValueSerializer;
        private readonly DuplicateColumnNameProcess _DuplicateColumnNameProcess;
        private readonly bool _DuplicateNameExceptionPreventUsed;
        public readonly DataTable DataTableWithoutData;
        public readonly Stream Stream;
        private long _StreamLengthWithValidData;
        private bool _DataReaderEof;

        public SqlStreamEngine(SqlDataReader p_DataReader, Stream p_Stream, ISqlValueSerializer p_SqlValueSerializer,
            DuplicateColumnNameProcess p_DuplicateColumnNameProcess)
        {
            _DataReader = p_DataReader;
            Stream = p_Stream;
            _SqlValueSerializer = p_SqlValueSerializer;
            _DuplicateColumnNameProcess = p_DuplicateColumnNameProcess;

            var table = _DataReader.GetSchemaTable();
            DataTableWithoutData = new DataTable();
            foreach (DataRow row in table.Rows)
            {
                var columnName = row["ColumnName"].ToString();
                var dataType = Type.GetType(row["DataType"].ToString()) ?? throw new Exception("SqlDataReader.GetSchemaTable must return column with name DataType");

                if ((_DuplicateColumnNameProcess == DuplicateColumnNameProcess.DuplicateColumnsWithNamePostfixWithData
                     || _DuplicateColumnNameProcess == DuplicateColumnNameProcess.DuplicateColumnsWithNamePostfixWithoutData)
                    && DataTableWithoutData.Columns.Contains(columnName))
                {
                    var uniqueColumnName = UniqueColumnName(columnName, DataTableWithoutData.Columns);
                    var column = new DataColumn(uniqueColumnName, dataType);
                    column.ExtendedProperties[OriginalColumnName] = columnName;
                    _DuplicateNameExceptionPreventUsed = true;
                    DataTableWithoutData.Columns.Add(column);
                    continue;
                }
                DataTableWithoutData.Columns.Add(new DataColumn(columnName, dataType));
            }
        }

        private string UniqueColumnName(string columnName, DataColumnCollection columns)
        {
            var i = 1;
            while (columns.Contains(columnName + "_" + i))
                i++;
            return columnName + "_" + i;
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            WriteDataToStreamFromDataReader(count);
            var readBytesFromStream = _StreamLengthWithValidData <= count ? _StreamLengthWithValidData : count;
            if (readBytesFromStream > 0)
            {
                Stream.Position = 0;
                Stream.Read(buffer, offset, (int) readBytesFromStream);
#if log
                LogBytes($"Stream readed {readBytesFromStream}B from buffer:", buffer, (int)readBytesFromStream);
#endif
                //Move unprocessed data for next Read operation and Stream.Position is prepared for next WriteDataToStreamFromDataReader
                if (readBytesFromStream < _StreamLengthWithValidData)
                    _StreamLengthWithValidData = MoveUnreadedDataToBeginOfStream();
                else
                    Stream.Position = _StreamLengthWithValidData = 0;
            }
            return (int) readBytesFromStream;
        }

        private int MoveUnreadedDataToBeginOfStream()
        {
            byte[] buffer = new byte[(int) (_StreamLengthWithValidData - Stream.Position)];
            Stream.Read(buffer, 0, buffer.Length);
            Stream.Position = 0;
            Stream.Write(buffer, 0, buffer.Length);
#if log
            LogStreamFromBeginToActualPosition("Moved unreaded data to begin of stream:", Stream);
#endif
            return buffer.Length;
        }

        private void WriteDataToStreamFromDataReader(int p_WriteMinBytes)
        {
            var dataReaderReaded = false;
            if (_StreamLengthWithValidData < p_WriteMinBytes)
                while (!_DataReaderEof)
                {
                    _DataReaderEof = !_DataReader.Read();
                    if (_DataReaderEof) break;
                    dataReaderReaded = true;

                    var count = _DataReader.FieldCount;
                    for (var i = 0; i < count; i++)
                    {
                        var val = _DataReader.GetValue(i);
#if log
                        Debug.WriteLine($"_DataReader.GetValue({i}) = {val}");
#endif
                        if (_DuplicateColumnNameProcess == DuplicateColumnNameProcess.DuplicateColumnsWithNamePostfixWithoutData
                            && _DuplicateNameExceptionPreventUsed
                            && DataTableWithoutData.Columns[i].ExtendedProperties[OriginalColumnName] != null)
                            val = string.Empty;

                        _SqlValueSerializer.WriteObject(Stream, val, DataTableWithoutData.Columns[i].DataType, i == count - 1);
                    }
#if log
                    LogStreamFromBeginToActualPosition("DataReader.Read written another DataRow to Stream. Stream data:", Stream);
#endif
                    if (Stream.Position >= p_WriteMinBytes) break;
                }
            if (dataReaderReaded)
                _StreamLengthWithValidData = Stream.Position;
        }

#if log
        private void LogStreamFromBeginToActualPosition(string p_Prefix, Stream p_Stream)
        {
            var pos = (int)p_Stream.Position;
            p_Stream.Position = 0;
            byte[] buffer = new byte[pos];
            p_Stream.Read(buffer, 0, pos);
            var text = Encoding.UTF8.GetString(buffer);
            p_Stream.Position = pos;

            Debug.WriteLine(p_Prefix + Environment.NewLine + text);
        }

        private void LogBytes(string p_Prefix, byte[] buffer, int count)
        {
            var text = Encoding.UTF8.GetString(buffer, 0, count);
            Debug.WriteLine(p_Prefix + Environment.NewLine + text);
        }
#endif
    }
}