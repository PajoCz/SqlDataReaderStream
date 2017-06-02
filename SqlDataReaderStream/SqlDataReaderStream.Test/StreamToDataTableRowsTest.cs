using System.Configuration;
using System.Data.SqlClient;
using NUnit.Framework;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream.Test
{
    [TestFixture]
    public class StreamToDataTableRowsTest
    {
        private static SqlConnection CreateConnection()
        {
            var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Database"].ConnectionString);
            conn.Open();
            return conn;
        }

        private void TestSqlValueSerializerForReadingAndWriting(ISqlValueSerializer p_SqlValueSerializer, int p_BufferSize = 8192)
        {
            var cmd = new SqlCommand("SELECT * FROM TestTable", CreateConnection());;

            //ACT with ASSERT
            using (var sqlDataReaderStream = new SqlStream(cmd, p_SqlValueSerializer))
            {
                var table = sqlDataReaderStream.DataTableWithoutData;
                Assert.AreEqual(0, table.Rows.Count);
                new StreamToDataTableRows().ReadStreamToDataTable(sqlDataReaderStream, table, p_SqlValueSerializer, p_BufferSize);
                Assert.AreEqual(3, table.Rows.Count);
                Assert.AreEqual(@"Test Tab	and NewLine
is ok", table.Rows[2]["TestString"], "Failed test string with special chars (same as RowSplitter and ColumnSplitter in Csv format)");
            }
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvSimple()
        {
            TestSqlValueSerializerForReadingAndWriting(new SqlValueSerializerCsvSimple());
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvSimple_SmallBuffer()
        {
            TestSqlValueSerializerForReadingAndWriting(new SqlValueSerializerCsvSimple(), 1);
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvWithXmlDictionaryWriter()
        {
            TestSqlValueSerializerForReadingAndWriting(new SqlValueSerializerCsvWithXmlDictionaryWriter());
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvWithXmlDictionaryWriter_SmallBuffer()
        {
            TestSqlValueSerializerForReadingAndWriting(new SqlValueSerializerCsvWithXmlDictionaryWriter(), 1);
        }
    }
}