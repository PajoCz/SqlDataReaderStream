using System.Configuration;
using System.Data;
using Microsoft.Data.SqlClient;
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
            var conn = ConnectHelper.CreateConnection();
            var sql = "SELECT * FROM TestTable";
            var cmd = new SqlCommand(sql, conn);

            Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));

            //ACT
            using (new SqlStream(cmd, new SqlValueSerializerCsvSimple()))
            {
            }

            //ASSERT
            Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
        }

        [Test]
        public void SqlValueSerializerCsvSimple()
        {
            using (var conn = ConnectHelper.CreateConnection())
            {
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
            using (var conn = ConnectHelper.CreateConnection())
            {
                var sql = "SELECT * FROM TestTable";
                var cmd = new SqlCommand(sql, conn);

                using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvWithXmlDictionaryWriter()))
                {
                    SaveToFile(FileName("TestTable-XmlDictionaryWriter.csv"), sqlDataReaderStream);
                }
            }
        }

        [Test]
        public void TwoColumnsSameName_Ctor_Default_DuplicateNameException_SqlCommandWithTransaction_ConnectionMustBeClosed()
        {
            using (var conn = ConnectHelper.CreateConnection())
            {
                using (var tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted))
                {
                    var sql = "SELECT *, 'A' as ColumnA, 'B' as ColumnA FROM TestTable";
                    var cmd = new SqlCommand(sql, conn, tran);

                    try
                    {
                        using (new SqlStream(cmd, new SqlValueSerializerCsvSimple()))
                        {
                        }
                    }
                    catch (DuplicateNameException)
                    {
                        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
                        return;
                    }
                    Assert.Fail("DuplicateNameException expected");
                }
            }
        }

        [Test]
        public void TwoColumnsSameName_Ctor_DuplicateColumnsWithNamePostfixWithoutData_CheckStreamToDataTableRows_ReadStreamToDataTable_SecondColumnWithoutValue()
        {
            var duplicateColumnNameProcess = DuplicateColumnNameProcess.DuplicateColumnsWithNamePostfixWithoutData;
            DataTable table = SelectDuplicateColumn(duplicateColumnNameProcess);
            Assert.That(table.Rows[0][1].ToString(), Is.EqualTo(string.Empty));
        }

        [Test]
        public void TwoColumnsSameName_Ctor_DuplicateColumnsWithNamePostfixWithData_CheckStreamToDataTableRows_ReadStreamToDataTable_SecondColumnWithValue()
        {
            var duplicateColumnNameProcess = DuplicateColumnNameProcess.DuplicateColumnsWithNamePostfixWithData;
            DataTable table = SelectDuplicateColumn(duplicateColumnNameProcess);
            Assert.That(table.Rows[0][1].ToString(), Is.EqualTo("B"));
        }

        private DataTable SelectDuplicateColumn(DuplicateColumnNameProcess p_DuplicateColumnNameProcess)
        {
            using (var conn = ConnectHelper.CreateConnection())
            {
                var sql = "SELECT 'A' as ColumnA, 'B' as ColumnA FROM TestTable";
                var cmd = new SqlCommand(sql, conn);

                using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvSimple(), p_DuplicateColumnNameProcess))
                {
                    Assert.That(sqlDataReaderStream.DataTableWithoutData.Columns.Count, Is.EqualTo(2));
                    Assert.That(sqlDataReaderStream.DataTableWithoutData.Columns[0].ColumnName, Is.EqualTo("ColumnA"));
                    Assert.That(sqlDataReaderStream.DataTableWithoutData.Columns[1].ColumnName, Is.EqualTo("ColumnA_1"));

                    //must read data to prepared table
                    DataTable table = sqlDataReaderStream.DataTableWithoutData;
                    new StreamToDataTableRows().ReadStreamToDataTable(sqlDataReaderStream, table, new SqlValueSerializerCsvSimple());
                    Assert.That(table.Rows[0][0].ToString(), Is.EqualTo("A"));
                    return table;
                }
            }
        }
    }
}