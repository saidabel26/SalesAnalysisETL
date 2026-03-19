namespace SalesAnalysisETL.Application.DTOs;

public class ExtractionSummaryDto
{
    public int CsvOrdersCount { get; set; }
    public int CsvOrderDetailsCount { get; set; }
    public int ApiCustomersCount { get; set; }
    public int ApiProductsCount { get; set; }
    public int HistoricalSalesCount { get; set; }
    public DateTime ExtractedAtUtc { get; set; }
}
