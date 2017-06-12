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
        public void SqlValueSerializerCsvSimple()
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
        public void SqlValueSerializerCsvWithXmlDictionaryWriter()
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

        [Test]
        public void TwoColumnsSameName_CtorThrowsDuplicateNameException_SqlCommandWithTransaction_ConnectionMustBeClosed()
        {
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Database"].ConnectionString))
            {
                conn.Open();
                using (var tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted))
                {
                    var sql = "SELECT *, 'A' as ColumnA, 'B' as ColumnA FROM TestTable";
                    var cmd = new SqlCommand(sql, conn, tran);

                    try
                    {
                        using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvSimple()))
                        {
                        }
                    }
                    catch (DuplicateNameException dne)
                    {
                        Assert.AreEqual(ConnectionState.Closed, conn.State);
                        return;
                    }
                    Assert.Fail("DuplicateNameException expected");
                }
            }
        }

        [Test]
        public void TwoColumnsSameName_CtorWithDuplicateNameExceptionPrevent_ReturnsOnlyFirstColumn()
        {
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Database"].ConnectionString))
            {
                conn.Open();
                var sql = "SELECT 'A' as ColumnA, 'B' as ColumnA FROM TestTable";
                var cmd = new SqlCommand(sql, conn);

                using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvSimple(), true))
                {
                    Assert.AreEqual(1, sqlDataReaderStream.DataTableWithoutData.Columns.Count);
                    Assert.AreEqual("ColumnA", sqlDataReaderStream.DataTableWithoutData.Columns[0].ColumnName);
                }
            }
        }

        [Test]
        public void TwoColumnsSameName_CtorThrowsDuplicateNameException_SqlCommandWithoutTransaction_ConnectionMustBeClosed()
        {
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Database"].ConnectionString))
            {
                conn.Open();
                var sql = "SELECT *, 'A' as ColumnA, 'B' as ColumnA FROM TestTable";
                var cmd = new SqlCommand(sql, conn);

                try
                {
                    using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvSimple()))
                    {
                    }
                }
                catch (DuplicateNameException dne)
                {
                    Assert.AreEqual(ConnectionState.Closed, conn.State);
                    return;
                }
                Assert.Fail("DuplicateNameException expected");
            }
        }
    }
}