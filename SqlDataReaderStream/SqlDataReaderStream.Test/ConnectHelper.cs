using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
