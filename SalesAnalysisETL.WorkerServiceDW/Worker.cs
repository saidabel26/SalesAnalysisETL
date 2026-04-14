using SalesAnalysisETL.Application.Interfaces.Services;

namespace SalesAnalysisETL.WorkerServiceDW
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IServiceScopeFactory _scopeFactory;

        public Worker(ILogger<Worker> logger, IServiceScopeFactory scopeFactory)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker ETL iniciado.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var extractionService = scope.ServiceProvider.GetRequiredService<IExtractionService>();

                    var summary = await extractionService.RunExtractionAsync(stoppingToken);

                    _logger.LogInformation(
                        "Resumen ETL: Extract => OrdersCSV={Orders}, OrderDetailsCSV={Details}, CustomersAPI={Customers}, ProductsAPI={Products}, SalesHistDB={Sales}; " +
                        "Transform => Dates={TDates}, Customers={TCust}, Products={TProd}, Status={TStatus}, Facts={TFacts}; " +
                        "Load => Dates+={LDates}, Customers+={LCustIns}/{LCustUpd}, Products+={LProdIns}/{LProdUpd}, Status+={LStatus}, Facts+={LFacts}; FechaUTC={DateUtc}",
                        summary.CsvOrdersCount,
                        summary.CsvOrderDetailsCount,
                        summary.ApiCustomersCount,
                        summary.ApiProductsCount,
                        summary.HistoricalSalesCount,
                        summary.TransformedDatesCount,
                        summary.TransformedCustomersCount,
                        summary.TransformedProductsCount,
                        summary.TransformedStatusesCount,
                        summary.TransformedFactsCount,
                        summary.LoadedDatesCount,
                        summary.LoadedCustomersInsertedCount,
                        summary.LoadedCustomersUpdatedCount,
                        summary.LoadedProductsInsertedCount,
                        summary.LoadedProductsUpdatedCount,
                        summary.LoadedStatusesCount,
                        summary.LoadedFactsCount,
                        summary.ExtractedAtUtc);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Ejecución cancelada por solicitud del host.");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error ejecutando el ETL.");
                }

                await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
            }

            _logger.LogInformation("Worker ETL finalizado.");
        }
    }
}
