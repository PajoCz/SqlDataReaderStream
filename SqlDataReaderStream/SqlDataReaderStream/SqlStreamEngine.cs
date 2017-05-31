using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream
{
    internal class SqlStreamEngine
    {
        private readonly SqlDataReader _DataReader;
        private readonly ISqlValueSerializer _SqlValueSerializer;
        public readonly DataTable DataTableWithoutData;
        public readonly Stream Stream;
        private int _MovedData;

        public SqlStreamEngine(SqlDataReader p_DataReader, Stream p_Stream, ISqlValueSerializer p_SqlValueSerializer, bool p_IncludeHeader)
        {
            _DataReader = p_DataReader;
            Stream = p_Stream;
            _SqlValueSerializer = p_SqlValueSerializer;

            var table = _DataReader.GetSchemaTable();
            DataTableWithoutData = new DataTable();
            foreach (DataRow row in table.Rows)
                DataTableWithoutData.Columns.Add(new DataColumn(row["ColumnName"].ToString(), Type.GetType(row["DataType"].ToString())));

            if (p_IncludeHeader)
            {
                for (var i = 0; i < table.Rows.Count; i++)
                    p_SqlValueSerializer.WriteObject(Stream, table.Rows[i][0], i == table.Rows.Count - 1);
                _MovedData = (int) Stream.Position;
            }
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            var writtenBytesToStream = WriteDataToStreamFromDataReader(count);
            var readBytesFromStream = writtenBytesToStream <= count ? writtenBytesToStream : count;
            if (readBytesFromStream > 0)
            {
                Stream.Read(buffer, offset, (int) readBytesFromStream);
                if (readBytesFromStream < writtenBytesToStream)
                    MoveUnreadedDataToBeginOfStream();
            }
            return (int)readBytesFromStream;
        }

        private void MoveUnreadedDataToBeginOfStream()
        {
            byte[] buffer = new byte[(int) (Stream.Length - Stream.Position)];
            Stream.Read(buffer, 0, buffer.Length);
            Stream.Position = 0;
            Stream.Write(buffer, 0, buffer.Length);
            _MovedData = buffer.Length;
        }

        private long WriteDataToStreamFromDataReader(int p_WriteMinBytes)
        {
            Stream.Position = _MovedData;
            while (_DataReader.Read() && Stream.Position < p_WriteMinBytes)
            {
                var count = _DataReader.FieldCount;
                for (var i = 0; i < count; i++)
                {
                    var val = _DataReader.GetValue(i);
                    _SqlValueSerializer.WriteObject(Stream, val, i == count - 1);
                }
            }
            var result = Stream.Position;
            Stream.Position = 0;
            _MovedData = 0;
            return result;
        }
    }
}