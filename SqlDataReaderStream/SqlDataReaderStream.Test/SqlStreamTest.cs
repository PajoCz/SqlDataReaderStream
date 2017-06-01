using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using NUnit.Framework;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream.Test
{
    [TestFixture]
    public partial class SqlStreamTest
    {
        [Test]
        public void AfterDisposeSqlStream_ConnectIsDisposed()
        {
            //ARRANGE
            var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Database"].ConnectionString);
            conn.Open();
            var sql = "SELECT * FROM TestTable";
            var cmd = new SqlCommand(sql, conn);

            Assert.AreEqual(ConnectionState.Open, conn.State);

            //ACT
            using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvSimple()))
            {
            }

            //ASSERT
            Assert.AreEqual(ConnectionState.Closed, conn.State);
        }

        [Test]
        public void AuditEventTable_SqlValueSerializerCsvSimple()
        {
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Database"].ConnectionString))
            {
                conn.Open();
                var sql = "SELECT * FROM TestTable";
                var cmd = new SqlCommand(sql, conn);

                using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvSimple()))
                {
                    SaveToFile(FileName("TestTable-CsvSimple.csv"), sqlDataReaderStream);
                }
            }
        }

        [Test]
        public void AuditEventTable_SqlValueSerializerCsvWithXmlDictionaryWriter()
        {
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Database"].ConnectionString))
            {
                conn.Open();
                var sql = "SELECT * FROM TestTable";
                var cmd = new SqlCommand(sql, conn);

                using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvWithXmlDictionaryWriter()))
                {
                    SaveToFile(FileName("TestTable-XmlDictionaryWriter.csv"), sqlDataReaderStream);
                }
            }
        }
    }
}