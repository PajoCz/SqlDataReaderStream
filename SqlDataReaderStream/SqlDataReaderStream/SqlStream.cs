using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream
{
    /// <summary>
    ///     Server-side Stream for reading data through SqlDataReader
    /// </summary>
    public class SqlStream : Stream
    {
        private readonly SqlDataReader _DataReader;
        private readonly SqlStreamEngine _StreamEngine;

        private long _Position;

        public SqlStream(SqlCommand p_SqlCommand, ISqlValueSerializer p_SqlValueSerializer)
        {
            _DataReader = p_SqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            _StreamEngine = new SqlStreamEngine(_DataReader, new MemoryStream(), p_SqlValueSerializer/*, false*/);
        }

        public DataTable DataTableWithoutData => _StreamEngine.DataTableWithoutData;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length { get; }

        public override long Position
        {
            get => _Position;
            set
            {
                if (_Position != value) Seek(value, SeekOrigin.Begin);
            }
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (buffer.Length - offset < count)
                throw new ArgumentException("Offset and length were out of bounds for the array");

            var readed = _StreamEngine.Read(buffer, offset, count);
            _Position += readed;
            return readed;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        protected override void Dispose(bool disposing)
        {
            _DataReader?.Close();
            _DataReader?.Dispose();
            base.Dispose(disposing);
        }
    }
}