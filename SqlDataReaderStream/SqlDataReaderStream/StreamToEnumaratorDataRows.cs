using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream
{
    public class StreamToEnumaratorDataRows : IEnumerator<DataRow>
    {
        private readonly byte[] _Buffer;
        private readonly List<TypeWithConverter> _ColumnTypes;
        private readonly DataTable _DataTable;
        private readonly Queue<DataRow> _Queue;
        private readonly ISqlValueSerializer _Serializer;
        private readonly Stream _Stream;
        private string _LastRowMayBeOnlyFirstFragment;

        public StreamToEnumaratorDataRows(Stream p_Stream, DataTable p_DataTable, ISqlValueSerializer p_Serializer, int p_BufferSize)
        {
            _Stream = p_Stream;
            _DataTable = p_DataTable;
            _Serializer = p_Serializer;
            _Buffer = new byte[p_BufferSize];
            _Queue = new Queue<DataRow>();
            _LastRowMayBeOnlyFirstFragment = string.Empty;
            _ColumnTypes = new List<TypeWithConverter>(p_DataTable.Columns.Count);
            foreach (DataColumn column in p_DataTable.Columns)
                _ColumnTypes.Add(new TypeWithConverter(column.DataType, TypeDescriptor.GetConverter(column.DataType)));
        }

        public void Dispose()
        {
            _Stream.Dispose();
        }

        public bool MoveNext()
        {
            if (_Queue.Count > 0)
            {
                Current = _Queue.Dequeue();
                return true;
            }

            // not data in queue, try read another from stream to queue
            var readed = int.MaxValue;
            while (readed > 0 && !_Queue.Any())
            {
                readed = _Stream.Read(_Buffer, 0, _Buffer.Length);
                var readedString = _LastRowMayBeOnlyFirstFragment + Encoding.UTF8.GetString(_Buffer, 0, readed);
                IEnumerable<object[]> readedSplitted = _Serializer.ReadValues(readedString, _ColumnTypes, out _LastRowMayBeOnlyFirstFragment);
                foreach (object[] rowData in readedSplitted)
                    _Queue.Enqueue(_DataTable.LoadDataRow(rowData, LoadOption.OverwriteChanges));
            }

            // returns first queue item to Current if any
            if (_Queue.Count > 0)
            {
                Current = _Queue.Dequeue();
                return true;
            }

            return false;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public DataRow Current { get; private set; }

        object IEnumerator.Current => Current;
    }
}