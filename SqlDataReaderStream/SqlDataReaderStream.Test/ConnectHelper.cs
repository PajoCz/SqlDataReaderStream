using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;

namespace SqlDataReaderStream.Test
{
    internal static class ConnectHelper
    {
        public static SqlConnection CreateConnection()
        {
            var dataDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var connectionString = ConfigurationManager.ConnectionStrings["Database"].ConnectionString
                .Replace("|DataDirectory|", dataDir);

            var conn = new SqlConnection(connectionString);
            conn.Open();
            return conn;
        }
    }
}