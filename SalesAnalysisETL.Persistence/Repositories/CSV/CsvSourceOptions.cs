namespace SalesAnalysisETL.Persistence.Repositories.CSV;

public class CsvSourceOptions
{
    public const string SectionName = "DataSources:Csv";

    public string OrdersFilePath { get; set; } = string.Empty;
    public string OrderDetailsFilePath { get; set; } = string.Empty;
}
