using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using NUnit.Framework;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream.Test
{
    [TestFixture]
    public class StreamToDataTableRowsTest
    {
        [Test]
        public void ReadStreamToDataTable_AuditEventRow_GuidColumn_DataStringEmpty()
        {
            //ARRANGE
            var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Oept"].ConnectionString);
            conn.Open();
            var sql = "SELECT TOP 1 * FROM dbo.AuditEvent";
            var cmd = new SqlCommand(sql, conn);

            DataTable table;
            //ACT with ASSERT
            using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvSimple()))
            {
                table = sqlDataReaderStream.DataTableWithoutData;
                Assert.AreEqual(0, table.Rows.Count);
                new StreamToDataTableRows().ReadStreamToDataTable(sqlDataReaderStream, table, new SqlValueSerializerCsvWithXmlDictionaryWriter());
                Assert.AreEqual(1, table.Rows.Count);
            }
        }
    }
}