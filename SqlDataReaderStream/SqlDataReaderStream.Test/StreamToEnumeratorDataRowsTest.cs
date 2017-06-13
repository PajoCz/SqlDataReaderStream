using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
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
                Assert.AreEqual(0, table.Rows.Count);
                List<DataRow> rows = new List<DataRow>();
                var oldPosition = sqlDataReaderStream.Position;
                foreach (var row in new StreamToEnumerableDataRows(sqlDataReaderStream, table, serializer, bufferSize))
                {
                    Assert.Greater(sqlDataReaderStream.Position, oldPosition);
                    oldPosition = sqlDataReaderStream.Position;
                    rows.Add(row);
                    Assert.AreEqual(ConnectionState.Open, cmd.Connection.State);
                }
                //after dispose foreach is disposed readed stream too and it close connection
                Assert.AreEqual(ConnectionState.Closed, cmd.Connection.State);
                Assert.AreEqual(3, rows.Count);
            }
        }
    }
}