namespace SalesAnalysisETL.Domain.Entities.CSV;

public class CsvOrder
{
    public int OrderId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public DateTime OrderDate { get; set; }
    public string Status { get; set; } = string.Empty;
}
