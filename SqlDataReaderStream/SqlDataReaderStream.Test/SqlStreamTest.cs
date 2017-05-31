using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
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
            var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Oept"].ConnectionString);
            conn.Open();
            var sql = "SELECT TOP 1 * FROM dbo.AuditEvent";
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
        public void AuditEventTable_SqlValueSerializerCsvWithXmlDictionaryWriter()
        {
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Oept"].ConnectionString))
            {
                conn.Open();
                var sql = "SELECT TOP 10000 * FROM dbo.AuditEvent";
                var cmd = new SqlCommand(sql, conn);

                using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvWithXmlDictionaryWriter()))
                {
                    SaveToFile(FileName("AuditEvent.csv"), sqlDataReaderStream);
                }
            }
        }

        [Test]
        public void AuditEventTable_SqlValueSerializerCsvSimple()
        {
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Oept"].ConnectionString))
            {
                conn.Open();
                var sql = "SELECT TOP 10000 * FROM dbo.AuditEvent";
                var cmd = new SqlCommand(sql, conn);

                using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvSimple()))
                {
                    SaveToFile(FileName("AuditEvent.csv"), sqlDataReaderStream);
                }
            }
        }

        [Test]
        public void Test_ViewOneRow()
        {
            //ARRANGE
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Oept"].ConnectionString))
            {
                conn.Open();
                var fn = FileName("View.csv");
                var sql =
                    "SELECT TOP 1000   Oept.v_rd_PozadavekOcenovanySubjekt.ZdaPrimarni_Template AS[Oept_v_rd_PozadavekOcenovanySubjekt_ZdaPrimarni_Template], Oept.v_rd_PozadavekOcenovanySubjekt.IC AS[Oept_v_rd_PozadavekOcenovanySubjekt_IC], Oept.v_rd_PozadavekOcenovanySubjekt.PrimarniKAM AS[Oept_v_rd_PozadavekOcenovanySubjekt_PrimarniKAM], Oept.v_rd_PozadavekOcenovanySubjekt.OcenovanySubjektTyp AS[Oept_v_rd_PozadavekOcenovanySubjekt_OcenovanySubjektTyp], Oept.v_rd_PozadavekOcenovanySubjekt.SubjektTyp AS[Oept_v_rd_PozadavekOcenovanySubjekt_SubjektTyp], Oept.v_rd_PozadavekOcenovanySubjekt.OcenovanySubjektNazev_Template AS[Oept_v_rd_PozadavekOcenovanySubjekt_OcenovanySubjektNazev_Template], Oept.v_rd_PozadavekOcenovanySubjekt.RatingSubjektu AS[Oept_v_rd_PozadavekOcenovanySubjekt_RatingSubjektu], Oept.v_rd_PozadavekOcenovanySubjekt.IdOcenovanySubjekt AS[Oept_v_rd_PozadavekOcenovanySubjekt_IdOcenovanySubjekt], Oept.v_rd_PozadavekOcenovanySubjekt.EditDisabled AS[Oept_v_rd_PozadavekOcenovanySubjekt_EditDisabled], Oept.v_rd_PozadavekOcenovanySubjekt.IdPozadavekOcenovanySubjekt AS[Oept_v_rd_PozadavekOcenovanySubjekt_IdPozadavekOcenovanySubjekt]  FROM Oept.v_rd_PozadavekOcenovanySubjekt WHERE(([Oept].[v_rd_PozadavekOcenovanySubjekt].[IdPozadavek] = 18775))";
                var cmd = new SqlCommand(sql, conn);

                //ACT
                using (var sqlDataReaderStream = new SqlStream(cmd, new SqlValueSerializerCsvWithXmlDictionaryWriter()))
                {
                    SaveToFile(fn, sqlDataReaderStream);
                }

                //ASSERT
                var content = File.ReadAllText(fn);
                var expected = "true\t25788001\tH22136\tReálný\tFirma\tVodafone Czech Republic a.s.\tE5\t4993\t1\t64431\n";
                Assert.AreEqual(expected, content);
            }
        }
    }
}