//#define log

using System;
using System.Data;
using System.Data.SqlClient;
#if log
using System.Diagnostics;
#endif
using System.IO;
#if log
using System.Text;
#endif
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream
{
    internal class SqlStreamEngine
    {
        private readonly SqlDataReader _DataReader;
        private readonly ISqlValueSerializer _SqlValueSerializer;
        public readonly DataTable DataTableWithoutData;
        public readonly Stream Stream;
        private long _StreamLengthWithValidData;
        private bool _DataReaderEof;

        public SqlStreamEngine(SqlDataReader p_DataReader, Stream p_Stream, ISqlValueSerializer p_SqlValueSerializer)
        {
            _DataReader = p_DataReader;
            Stream = p_Stream;
            _SqlValueSerializer = p_SqlValueSerializer;

            var table = _DataReader.GetSchemaTable();
            DataTableWithoutData = new DataTable();
            foreach (DataRow row in table.Rows)
                DataTableWithoutData.Columns.Add(new DataColumn(row["ColumnName"].ToString(), Type.GetType(row["DataType"].ToString())));
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
                LogBytes($"Stream readed {readBytesFromStream}B from buffer : ", buffer, (int)readBytesFromStream);
#endif
                //Move unprocessed data for next Read operation and Stream.Position is prepared for next WriteDataToStreamFromDataReader
                _StreamLengthWithValidData = readBytesFromStream < _StreamLengthWithValidData
                    ? MoveUnreadedDataToBeginOfStream() 
                    : 0;
            }
            return (int)readBytesFromStream;
        }

        private int MoveUnreadedDataToBeginOfStream()
        {
            byte[] buffer = new byte[(int) (_StreamLengthWithValidData - Stream.Position)];
            Stream.Read(buffer, 0, buffer.Length);
            Stream.Position = 0;
            Stream.Write(buffer, 0, buffer.Length);
#if log
            LogStreamFromBeginToActualPosition("Moved unreaded data to begin of stream : ", Stream);
#endif
            return buffer.Length;
        }

        private void WriteDataToStreamFromDataReader(int p_WriteMinBytes)
        {
            bool dataReaderReaded = false;
            if (_StreamLengthWithValidData < p_WriteMinBytes)
            {
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
                        _SqlValueSerializer.WriteObject(Stream, val, DataTableWithoutData.Columns[i].DataType, i == count - 1);
                    }
#if log
                    LogStreamFromBeginToActualPosition("DataReader.Read written DataRows to Stream. Stream = ", Stream);
#endif
                    if (Stream.Position >= p_WriteMinBytes) break;
                }
            }
            if (dataReaderReaded)
            {
                _StreamLengthWithValidData = Stream.Position;
            }
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

            Debug.WriteLine(p_Prefix + text);
        }

        private void LogBytes(string p_Prefix, byte[] buffer, int count)
        {
            var text = Encoding.UTF8.GetString(buffer, 0, count);
            Debug.WriteLine(p_Prefix + text);
        }
#endif
    }
}