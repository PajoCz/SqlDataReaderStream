using System.Configuration;
using System.Data.SqlClient;

namespace SqlDataReaderStream.Test
{
    internal static class ConnectHelper
    {
        public static SqlConnection CreateConnection()
        {
            var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Database"].ConnectionString);
            conn.Open();
            return conn;
        }
    }
}