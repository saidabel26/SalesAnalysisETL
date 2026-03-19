namespace SalesAnalysisETL.Persistence.Repositories.API;

public class ApiSourceOptions
{
    public const string SectionName = "DataSources:Api";

    public string BaseUrl { get; set; } = string.Empty;
    public string CustomersEndpoint { get; set; } = "/api/customers";
    public string ProductsEndpoint { get; set; } = "/api/products";
}
