namespace SqlDataReaderStream
{
    public enum DuplicateColumnNameProcess
    {
        DuplicateNameException,
        DuplicateColumnsWithNamePostfixWithoutData,
        DuplicateColumnsWithNamePostfixWithData
    }
}