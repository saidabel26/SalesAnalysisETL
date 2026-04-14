namespace SalesAnalysisETL.Application.DTOs;

public class ExtractionSummaryDto
{
    public int CsvOrdersCount { get; set; }
    public int CsvOrderDetailsCount { get; set; }
    public int ApiCustomersCount { get; set; }
    public int ApiProductsCount { get; set; }
    public int HistoricalSalesCount { get; set; }

    public int TransformedDatesCount { get; set; }
    public int TransformedCustomersCount { get; set; }
    public int TransformedProductsCount { get; set; }
    public int TransformedStatusesCount { get; set; }
    public int TransformedFactsCount { get; set; }

    public int LoadedDatesCount { get; set; }
    public int LoadedCustomersInsertedCount { get; set; }
    public int LoadedCustomersUpdatedCount { get; set; }
    public int LoadedProductsInsertedCount { get; set; }
    public int LoadedProductsUpdatedCount { get; set; }
    public int LoadedStatusesCount { get; set; }
    public int LoadedFactsCount { get; set; }

    public DateTime ExtractedAtUtc { get; set; }
}
