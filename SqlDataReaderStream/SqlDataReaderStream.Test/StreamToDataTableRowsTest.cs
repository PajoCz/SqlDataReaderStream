using System.Configuration;
using System.Data.SqlClient;
using NUnit.Framework;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream.Test
{
    [TestFixture]
    public class StreamToDataTableRowsTest
    {
        private static void TestSqlValueSerializer(SqlCommand cmd, ISqlValueSerializer sqlValueSerializer)
        {
            using (var sqlDataReaderStream = new SqlStream(cmd, sqlValueSerializer))
            {
                var table = sqlDataReaderStream.DataTableWithoutData;
                Assert.AreEqual(0, table.Rows.Count);
                new StreamToDataTableRows().ReadStreamToDataTable(sqlDataReaderStream, table, sqlValueSerializer);
                Assert.AreEqual(3, table.Rows.Count);
                Assert.AreEqual(@"Test Tab	and NewLine
is ok", table.Rows[2]["TestString"], "Failed test string with special chars (same as RowSplitter and ColumnSplitter in Csv format)");
            }
        }

        private static SqlConnection CreateConnection()
        {
            var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Database"].ConnectionString);
            conn.Open();
            return conn;
        }

        private static SqlCommand CreateCommandWithNewConnect(string sql)
        {
            var cmd = new SqlCommand(sql, CreateConnection());
            return cmd;
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvSimple()
        {
            //ARRANGE
            var cmd = CreateCommandWithNewConnect("SELECT * FROM TestTable");

            //ACT with ASSERT
            TestSqlValueSerializer(cmd, new SqlValueSerializerCsvSimple());
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvWithXmlDictionaryWriter()
        {
            //ARRANGE
            var cmd = CreateCommandWithNewConnect("SELECT * FROM TestTable");

            //ACT with ASSERT
            TestSqlValueSerializer(cmd, new SqlValueSerializerCsvWithXmlDictionaryWriter());
        }
    }
}