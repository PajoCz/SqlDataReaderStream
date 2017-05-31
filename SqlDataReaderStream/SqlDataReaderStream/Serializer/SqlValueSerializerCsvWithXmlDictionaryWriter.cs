using System;
using System.IO;
using System.Text;
using System.Xml;

namespace SqlDataReaderStream.Serializer
{
    public class SqlValueSerializerCsvWithXmlDictionaryWriter : SqlValueSerializerCsvBase, ISqlValueSerializer
    {
        private readonly byte[] _EndLineBuffer;
        private readonly byte[] _SplitterBuffer;
        private readonly Stream _StreamXml;
        private readonly XmlDictionaryWriter _XmlDictionaryWriter;
        private byte[] _Buffer;

        public SqlValueSerializerCsvWithXmlDictionaryWriter(Encoding p_Encoding) : base(p_Encoding)
        {
            _StreamXml = new MemoryStream();
            _XmlDictionaryWriter = XmlDictionaryWriter.CreateTextWriter(_StreamXml, p_Encoding, false);
            _EndLineBuffer = Encoding.UTF8.GetBytes(RowSplitter.ToString());
            _SplitterBuffer = Encoding.UTF8.GetBytes(ColumnSplitter.ToString());
            _Buffer = new byte[0];
        }

        public SqlValueSerializerCsvWithXmlDictionaryWriter() : this(Encoding.UTF8)
        {
        }

        public void WriteObject(Stream p_Stream, object p_Value, bool p_LastValueOfRow)
        {
            //DataContractSerializer dcs = new DataContractSerializer(p_Value.GetType());
            //dcs.WriteObject(p_Stream, p_Value);
            _StreamXml.Position = 0;
            WriteOneObjectByXmlDictionaryWriter(_XmlDictionaryWriter, p_Value);
            CopyValueFromStreamXmlToStream(_StreamXml, p_Stream, ref _Buffer);
            WriteSplitter(p_Stream, p_LastValueOfRow);
        }

        private void WriteSplitter(Stream p_Stream, bool p_LastValueOfRow)
        {
            if (p_LastValueOfRow)
                p_Stream.Write(_EndLineBuffer, 0, _EndLineBuffer.Length);
            else
                p_Stream.Write(_SplitterBuffer, 0, _SplitterBuffer.Length);
        }

        private static void CopyValueFromStreamXmlToStream(Stream p_StreamXml, Stream p_Stream, ref byte[] p_Buffer)
        {
            if (p_Buffer.Length < p_StreamXml.Position)
                Array.Resize(ref p_Buffer, (int) p_StreamXml.Position + 2);
            if (p_StreamXml.Position != "<a/>".Length)
            {
                var valueLength = (int) p_StreamXml.Position - "<a>".Length - "</a>".Length;
                p_StreamXml.Position = "<a>".Length;
                p_StreamXml.Read(p_Buffer, 0, (int) (p_StreamXml.Length - p_StreamXml.Position) - "</a>".Length);
                p_Stream.Write(p_Buffer, 0, valueLength);
            }
        }

        private static void WriteOneObjectByXmlDictionaryWriter(XmlDictionaryWriter p_XmlDictionaryWriter, object p_Value)
        {
            p_XmlDictionaryWriter.WriteStartElement("a");
            if (p_Value == DBNull.Value)
                p_Value = string.Empty;
            p_XmlDictionaryWriter.WriteValue(p_Value);
            p_XmlDictionaryWriter.WriteEndElement();
            p_XmlDictionaryWriter.Flush();
        }
    }
}