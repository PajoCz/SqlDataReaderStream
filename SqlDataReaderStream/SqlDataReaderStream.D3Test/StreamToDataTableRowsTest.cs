using System.Configuration;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using SqlDataReaderStream.Serializer;

namespace SqlDataReaderStream.D3Test
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
        
        private void TestSqlValueSerializerForReadingAndWriting(ISqlValueSerializer p_SqlValueSerializer, int p_BufferSize)
        {
            var sql = @"
DECLARE @p0 dbo.tt_ReportManagerSelectList
INSERT INTO @p0 VALUES('CisloPozadavku','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('Mena','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('Tarif','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('Sezona','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('CK','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('CKa','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('P1','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('KrNC','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('PNC','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('KM','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('KC','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('PM','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('PC','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('SpotrebaMWh','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('VaR','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('VaRZaMWh','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('ZdaAkceptovany','Neagregovan','','False','9')
INSERT INTO @p0 VALUES('PocetDnu','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('Poznamka','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('IdOceneni','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('Produkt','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('PozadavekCilOceneni','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('IdDiagram_Template','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('IdDiagramBurza_Template','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('IdOceneniCenovyElement','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('IdOceneniStav','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('IdPozadavek','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('Obor','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('IdOceneniDiagram','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('IdOceneniTyp','Neagregovan','','False','1')
INSERT INTO @p0 VALUES('WorkflowInstanceState','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('WorkflowProcesState','Neagregovan','','False','0')
INSERT INTO @p0 VALUES('ViditelnostDleStavuOceneni','Neagregovan','','False','9')
INSERT INTO @p0 VALUES('IdPozadavekTypNakupu','Neagregovan','','False','1')
DECLARE @p1 dbo.tt_ReportManagerFilterList
INSERT INTO @p1 VALUES('o.IdOceneniStav','JeVSeznamu','7','','0')
INSERT INTO @p1 VALUES('ob.IdObor','JeVSeznamu','2','','0')
DECLARE @p2 dbo.tt_ReportManagerOrderBy
INSERT INTO @p2 VALUES('IdOceneni','Sestupne')
INSERT INTO @p2 VALUES('Mena','Vzestupne')
EXECUTE Oept.p_rd_OceneniAkceptace @SelectList=@p0,@FilterList=@p1,@OrderBy=@p2,@SelectTop=10000,@Aggregation=0,@UserName='rmacejik'
";
            var cmd = new SqlCommand(sql, CreateConnection());

            //ACT with ASSERT
            using (var sqlDataReaderStream = new SqlStream(cmd, p_SqlValueSerializer))
            {
                
                var table = sqlDataReaderStream.DataTableWithoutData;
                Assert.That(table.Rows.Count, Is.EqualTo(0));
                new StreamToDataTableRows().ReadStreamToDataTable(sqlDataReaderStream, table, p_SqlValueSerializer, p_BufferSize);
                Assert.That(table.Rows.Count, Is.EqualTo(6194));
            }
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvSimple_Buffer80kB()
        {
            TestSqlValueSerializerForReadingAndWriting(new SqlValueSerializerCsvSimple(), 80 * 1024);
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvSimple_Buffer8kB()
        {
            TestSqlValueSerializerForReadingAndWriting(new SqlValueSerializerCsvSimple(), 8 * 1024);
        }

        [Test]
        public void ReadStreamToDataTable_SqlValueSerializerCsvSimple_Buffer1kB()
        {
            TestSqlValueSerializerForReadingAndWriting(new SqlValueSerializerCsvSimple(), 1 * 1024);
        }
    }
}