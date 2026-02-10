using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream.Test
{
    [TestFixture]
    public class StreamToEnumeratorDataRowsTest
    {
        [Test]
        public void Enumerator_ReadInputStreamToDataRowsPerPartes_WithSmallBuffer_CheckReadedAllRows_StreamAndItsConnectIsClosedAfterForeach()
        {
            var cmd = new SqlCommand("SELECT * FROM TestTable", ConnectHelper.CreateConnection());
            ISqlValueSerializer serializer = new SqlValueSerializerCsvSimple();
            var bufferSize = 10;

            //ACT with ASSERT
            using (var sqlDataReaderStream = new SqlStream(cmd, serializer))
            {
                var table = sqlDataReaderStream.DataTableWithoutData;
                Assert.That(table.Rows.Count, Is.EqualTo(0));
                List<DataRow> rows = new List<DataRow>();
                var oldPosition = sqlDataReaderStream.Position;
                foreach (var row in new StreamToEnumerableDataRows(sqlDataReaderStream, table, serializer, bufferSize))
                {
                    Assert.That(sqlDataReaderStream.Position, Is.GreaterThan(oldPosition));
                    oldPosition = sqlDataReaderStream.Position;
                    rows.Add(row);
                    Assert.That(cmd.Connection.State, Is.EqualTo(ConnectionState.Open));
                }
                //after dispose foreach is disposed readed stream too and it close connection
                Assert.That(cmd.Connection.State, Is.EqualTo(ConnectionState.Closed));
                Assert.That(rows.Count, Is.EqualTo(3));
            }
        }
    }
}