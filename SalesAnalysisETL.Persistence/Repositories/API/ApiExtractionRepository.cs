using System.Net.Http.Json;
using Microsoft.Extensions.Options;
using SalesAnalysisETL.Application.Interfaces.Repositories;
using SalesAnalysisETL.Domain.Entities.API;

namespace SalesAnalysisETL.Persistence.Repositories.API;

public class ApiExtractionRepository : IApiExtractionRepository
{
    private readonly HttpClient _httpClient;
    private readonly ApiSourceOptions _options;

    public ApiExtractionRepository(HttpClient httpClient, IOptions<ApiSourceOptions> options)
    {
        _httpClient = httpClient;
        _options = options.Value;
    }

    public async Task<(IReadOnlyCollection<ApiCustomer> Customers, IReadOnlyCollection<ApiProduct> Products)> ExtractAsync(CancellationToken cancellationToken)
    {
        var customersTask = _httpClient.GetFromJsonAsync<List<ApiCustomer>>(_options.CustomersEndpoint, cancellationToken);
        var productsTask = _httpClient.GetFromJsonAsync<List<ApiProduct>>(_options.ProductsEndpoint, cancellationToken);

        await Task.WhenAll(customersTask!, productsTask!);

        var customers = await customersTask ?? new List<ApiCustomer>();
        var products = await productsTask ?? new List<ApiProduct>();

        return (customers, products);
    }
}
