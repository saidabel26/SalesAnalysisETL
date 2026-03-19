namespace SalesAnalysisETL.Domain.Entities.CSV;

public class CsvOrderDetail
{
    public int OrderId { get; set; }
    public int ProductId { get; set; }
    public int Quantity { get; set; }
    public decimal TotalPrice { get; set; }
}
