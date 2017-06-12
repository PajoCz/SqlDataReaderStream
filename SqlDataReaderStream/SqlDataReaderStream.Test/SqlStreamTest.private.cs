using System.Configuration;
using System.IO;
using System.IO.Compression;

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
            //using( FileStream fsRead = new FileStream(p_FileName, FileMode.Open))
            //using (FileStream fs = new FileStream(Path.ChangeExtension(p_FileName, ".gzip"), FileMode.Create))
            //using (GZipStream zipStream = new GZipStream(fs, CompressionMode.Compress))
            //{
            //    fsRead.CopyTo(zipStream);
            //}

            //using (FileStream fsRead = new FileStream(p_FileName, FileMode.Open))
            //using (FileStream fs = new FileStream(Path.ChangeExtension(p_FileName, "-buffer.gzip"), FileMode.Create))
            //using (GZipStream zipStream = new GZipStream(fs, CompressionMode.Compress))
            //using (BufferedStream bufferedStream = new BufferedStream(zipStream, 8 * 1024))
            //{
            //    fsRead.CopyTo(bufferedStream);
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