using System.IO;

namespace SqlDataReaderStream.Serializer
{
    public interface ISqlValueSerializer : ISqlValueSerializerReadValues
    {
        void WriteObject(Stream p_Stream, object p_Value, bool p_LastValueOfRow);
    }
}