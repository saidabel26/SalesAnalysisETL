using SalesAnalysisETL.Application.Interfaces.Services;

namespace SalesAnalysisETL.WorkerServiceDW
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IExtractionService _extractionService;

        public Worker(ILogger<Worker> logger, IExtractionService extractionService)
        {
            _logger = logger;
            _extractionService = extractionService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker ETL iniciado.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var summary = await _extractionService.RunExtractionAsync(stoppingToken);

                    _logger.LogInformation(
                        "Resumen extracción: OrdersCSV={Orders}, OrderDetailsCSV={Details}, CustomersAPI={Customers}, ProductsAPI={Products}, SalesHistDB={Sales}, FechaUTC={DateUtc}",
                        summary.CsvOrdersCount,
                        summary.CsvOrderDetailsCount,
                        summary.ApiCustomersCount,
                        summary.ApiProductsCount,
                        summary.HistoricalSalesCount,
                        summary.ExtractedAtUtc);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Ejecución cancelada por solicitud del host.");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error ejecutando la extracción del ETL.");
                }

                await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
            }

            _logger.LogInformation("Worker ETL finalizado.");
        }
    }
}
