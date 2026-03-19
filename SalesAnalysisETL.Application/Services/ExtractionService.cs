using Microsoft.Extensions.Logging;
using SalesAnalysisETL.Application.DTOs;
using SalesAnalysisETL.Application.Interfaces.Repositories;
using SalesAnalysisETL.Application.Interfaces.Services;

namespace SalesAnalysisETL.Application.Services;

public class ExtractionService : IExtractionService
{
    private readonly ICsvExtractionRepository _csvExtractionRepository;
    private readonly IApiExtractionRepository _apiExtractionRepository;
    private readonly IHistoricalSalesRepository _historicalSalesRepository;
    private readonly ILogger<ExtractionService> _logger;

    public ExtractionService(
        ICsvExtractionRepository csvExtractionRepository,
        IApiExtractionRepository apiExtractionRepository,
        IHistoricalSalesRepository historicalSalesRepository,
        ILogger<ExtractionService> logger)
    {
        _csvExtractionRepository = csvExtractionRepository;
        _apiExtractionRepository = apiExtractionRepository;
        _historicalSalesRepository = historicalSalesRepository;
        _logger = logger;
    }

    public async Task<ExtractionSummaryDto> RunExtractionAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando extracción paralela desde CSV, API y base de datos histórica.");

        var csvTask = _csvExtractionRepository.ExtractAsync(cancellationToken);
        var apiTask = _apiExtractionRepository.ExtractAsync(cancellationToken);
        var dbTask = _historicalSalesRepository.ExtractAsync(cancellationToken);

        await Task.WhenAll(csvTask, apiTask, dbTask);

        var csvResult = await csvTask;
        var apiResult = await apiTask;
        var dbResult = await dbTask;

        var summary = new ExtractionSummaryDto
        {
            CsvOrdersCount = csvResult.Orders.Count,
            CsvOrderDetailsCount = csvResult.OrderDetails.Count,
            ApiCustomersCount = apiResult.Customers.Count,
            ApiProductsCount = apiResult.Products.Count,
            HistoricalSalesCount = dbResult.Count,
            ExtractedAtUtc = DateTime.UtcNow
        };

        _logger.LogInformation(
            "Extracción completada. CSV: Orders={Orders}, OrderDetails={Details}. API: Customers={Customers}, Products={Products}. DB histórica: Sales={Sales}.",
            summary.CsvOrdersCount,
            summary.CsvOrderDetailsCount,
            summary.ApiCustomersCount,
            summary.ApiProductsCount,
            summary.HistoricalSalesCount);

        return summary;
    }
}
