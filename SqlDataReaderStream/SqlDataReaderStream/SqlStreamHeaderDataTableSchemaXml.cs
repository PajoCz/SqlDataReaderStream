using System;
using System.Data;
using System.IO;

namespace SqlDataReaderStream
{
    public class SqlStreamHeaderDataTableSchemaXml : ISqlStreamHeader
    {
        public void WriteToStream(Stream p_Stream, DataTable p_DataTableWithoutData)
        {
            DataSet ds = new DataSet("ds");
            p_DataTableWithoutData.TableName = "dt";
            ds.Tables.Add(p_DataTableWithoutData);
            //write 4 bytes as length - now unknown, write zero
            int dsLength = 0;
            byte[] dsLengthBytes = BitConverter.GetBytes(dsLength);
            p_Stream.Write(dsLengthBytes, 0, dsLengthBytes.Length);
            //serialize schema of p_DataTableWitoutData
            ds.WriteXml(p_Stream, XmlWriteMode.WriteSchema);
            //check written bytes
            var lenPos = p_Stream.Position;
            dsLength = (int) p_Stream.Position - 4;
            dsLengthBytes = BitConverter.GetBytes(dsLength);
            //write written length as first 4 bytes
            p_Stream.Position = 0;
            p_Stream.Write(dsLengthBytes, 0, dsLengthBytes.Length);
            p_Stream.Position = lenPos;
        }

        public object ReadHeader(Stream p_Stream, int p_BufferSize = StreamToDataTableRows.DefaultCopyBufferSize)
        {
            DataSet ds = new DataSet();
            //read size as integer
            byte[] schemaLenBytes = new byte[4];
            p_Stream.Read(schemaLenBytes, 0, 4);
            int schemaLen = BitConverter.ToInt32(schemaLenBytes, 0);
            //copy from p_Stream (limited bytes) to new MemoryStream and deserialize schema and create emtpy Table
            using (MemoryStream ms = new MemoryStream())
            {
                SqlStreamEngine.CopyStreamWithLengthLimit(p_Stream, ms, schemaLen, p_BufferSize);
                ms.Position = 0;
                ds.ReadXmlSchema(ms);
            }
            return ds.Tables[0];
        }
    }
}