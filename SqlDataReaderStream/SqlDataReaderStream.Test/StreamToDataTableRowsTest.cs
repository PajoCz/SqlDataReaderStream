using System.Data.SqlClient;
using NUnit.Framework;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream.Test
{
    [TestFixture]
    public class StreamToDataTableRowsTest
    {
        private void TestSqlValueSerializerForReadingAndWriting(ISqlValueSerializer p_SqlValueSerializer, int p_BufferSize = 8192)
        {
            var cmd = new SqlCommand("SELECT * FROM TestTable", ConnectHelper.CreateConnection());

            //ACT with ASSERT
            using (var sqlDataReaderStream = new SqlStream(cmd, p_SqlValueSerializer))
            {
                var table = sqlDataReaderStream.DataTableWithoutData;
                Assert.That(table.Rows.Count, Is.EqualTo(0));
                new StreamToDataTableRows().ReadStreamToDataTable(sqlDataReaderStream, table, p_SqlValueSerializer, p_BufferSize);
                Assert.That(table.Rows.Count, Is.EqualTo(3));
                Assert.That(table.Rows[2]["TestString"], Is.EqualTo(@"Test Tab	and NewLine
is ok"), "Failed test string with special chars (same as RowSplitter and ColumnSplitter in Csv format)");
            }
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvSimple()
        {
            TestSqlValueSerializerForReadingAndWriting(new SqlValueSerializerCsvSimple());
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvSimple_BufferSizeSameAsOneRowSerialized()
        {
            TestSqlValueSerializerForReadingAndWriting(new SqlValueSerializerCsvSimple(), 7);
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