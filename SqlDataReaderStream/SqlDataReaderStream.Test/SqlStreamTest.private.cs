using System.Configuration;
using System.IO;

namespace SqlDataReaderStream.Test
{
    public partial class SqlStreamTest
    {
        private void SaveToFile(string p_FileName, Stream p_Stream)
        {
            using (var fs = new FileStream(p_FileName, FileMode.Create))
            {
                p_Stream.CopyTo(fs);
            }

            ////gzip
            //p_Stream.Position = 0;
            //using (FileStream fs = new FileStream(Path.ChangeExtension(p_FileName, ".gzip"), FileMode.Create))
            //using (GZipStream zipStream = new GZipStream(fs, CompressionMode.Compress))
            //{
            //    p_Stream.CopyTo(zipStream);
            //}
        }

        private static string FileName(string p_FileNameWithoutDirectory)
        {
            var dir = ConfigurationManager.AppSettings["DataDir"];
            Directory.CreateDirectory(dir);
            return Path.Combine(dir, p_FileNameWithoutDirectory);
        }
    }
}