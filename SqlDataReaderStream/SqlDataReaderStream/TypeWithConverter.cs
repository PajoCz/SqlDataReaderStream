using System;
using System.ComponentModel;

namespace SqlDataReaderStream
{
    public class TypeWithConverter
    {
        public TypeWithConverter(Type p_Type, TypeConverter p_TypeConverter)
        {
            Type = p_Type;
            TypeConverter = p_TypeConverter;
        }

        public Type Type { get; set; }
        public TypeConverter TypeConverter { get; set; }
    }
}