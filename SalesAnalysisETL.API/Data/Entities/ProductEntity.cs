namespace SalesAnalysisETL.API.Data.Entities;

public class ProductEntity
{
    public int ProductId { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public int CategoryId { get; set; }
    public string CategoryName { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Stock { get; set; }
}
