using System.Globalization;
using Microsoft.Extensions.Logging;
using SalesAnalysisETL.Application.DTOs;
using SalesAnalysisETL.Application.Interfaces.Repositories;
using SalesAnalysisETL.Application.Interfaces.Services;
using SalesAnalysisETL.Domain.Entities.API;
using SalesAnalysisETL.Domain.Entities.CSV;
using SalesAnalysisETL.Domain.Entities.DB;

namespace SalesAnalysisETL.Application.Services;

public class ExtractionService : IExtractionService
{
    private readonly ICsvExtractionRepository _csvExtractionRepository;
    private readonly IApiExtractionRepository _apiExtractionRepository;
    private readonly IHistoricalSalesRepository _historicalSalesRepository;
    private readonly IDataWarehouseRepository _dataWarehouseRepository;
    private readonly ILogger<ExtractionService> _logger;

    public ExtractionService(
        ICsvExtractionRepository csvExtractionRepository,
        IApiExtractionRepository apiExtractionRepository,
        IHistoricalSalesRepository historicalSalesRepository,
        IDataWarehouseRepository dataWarehouseRepository,
        ILogger<ExtractionService> logger)
    {
        _csvExtractionRepository = csvExtractionRepository;
        _apiExtractionRepository = apiExtractionRepository;
        _historicalSalesRepository = historicalSalesRepository;
        _dataWarehouseRepository = dataWarehouseRepository;
        _logger = logger;
    }

    public async Task<ExtractionSummaryDto> RunExtractionAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando ETL (extracción, transformación y carga).\n");

        var csvTask = _csvExtractionRepository.ExtractAsync(cancellationToken);
        var apiTask = _apiExtractionRepository.ExtractAsync(cancellationToken);
        var dbTask = _historicalSalesRepository.ExtractAsync(cancellationToken);

        await Task.WhenAll(csvTask, apiTask, dbTask);

        var csvResult = await csvTask;
        var apiResult = await apiTask;
        var dbResult = await dbTask;

        var payload = Transform(
            csvResult.Orders,
            csvResult.OrderDetails,
            apiResult.Customers,
            apiResult.Products,
            dbResult);

        var loadSummary = await _dataWarehouseRepository.LoadAsync(payload, cancellationToken);

        var summary = new ExtractionSummaryDto
        {
            CsvOrdersCount = csvResult.Orders.Count,
            CsvOrderDetailsCount = csvResult.OrderDetails.Count,
            ApiCustomersCount = apiResult.Customers.Count,
            ApiProductsCount = apiResult.Products.Count,
            HistoricalSalesCount = dbResult.Count,
            TransformedDatesCount = payload.Dates.Count,
            TransformedCustomersCount = payload.Customers.Count,
            TransformedProductsCount = payload.Products.Count,
            TransformedStatusesCount = payload.Statuses.Count,
            TransformedFactsCount = payload.Facts.Count,
            LoadedDatesCount = loadSummary.DatesInsertedCount,
            LoadedCustomersInsertedCount = loadSummary.CustomersInsertedCount,
            LoadedCustomersUpdatedCount = loadSummary.CustomersUpdatedCount,
            LoadedProductsInsertedCount = loadSummary.ProductsInsertedCount,
            LoadedProductsUpdatedCount = loadSummary.ProductsUpdatedCount,
            LoadedStatusesCount = loadSummary.StatusesInsertedCount,
            LoadedFactsCount = loadSummary.FactsInsertedCount,
            ExtractedAtUtc = DateTime.UtcNow
        };

        _logger.LogInformation(
            "ETL completado. Extracción: Orders={Orders}, Details={Details}, Customers={Customers}, Products={Products}, Hist={Hist}. " +
            "Transformación: Dates={Dates}, Customers={TCust}, Products={TProd}, Status={TStatus}, Facts={TFacts}. " +
            "Carga: Dates+={LDate}, Customers+={LCustIns}/{LCustUpd}, Products+={LProdIns}/{LProdUpd}, Status+={LStatus}, Facts+={LFacts}.",
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
            summary.LoadedFactsCount);

        return summary;
    }

    private static EtlPayloadDto Transform(
        IReadOnlyCollection<CsvOrder> csvOrders,
        IReadOnlyCollection<CsvOrderDetail> csvOrderDetails,
        IReadOnlyCollection<ApiCustomer> apiCustomers,
        IReadOnlyCollection<ApiProduct> apiProducts,
        IReadOnlyCollection<HistoricalSaleRecord> historicalRecords)
    {
        var customers = apiCustomers
            .Where(c => !string.IsNullOrWhiteSpace(c.CustomerId))
            .GroupBy(c => Normalize(c.CustomerId), StringComparer.OrdinalIgnoreCase)
            .Select(g =>
            {
                var c = g.First();
                var firstName = Normalize(c.FirstName);
                var lastName = Normalize(c.LastName);
                return new DimCustomerDto
                {
                    CustomerId = Normalize(c.CustomerId),
                    FirstName = firstName,
                    LastName = lastName,
                    FullName = Normalize($"{firstName} {lastName}"),
                    Email = Normalize(c.Email),
                    Phone = Normalize(c.Phone),
                    City = Normalize(c.City),
                    Country = Normalize(c.Country)
                };
            })
            .ToList();

        var products = apiProducts
            .Where(p => p.ProductId > 0)
            .GroupBy(p => p.ProductId)
            .Select(g =>
            {
                var p = g.First();
                return new DimProductDto
                {
                    ProductId = p.ProductId,
                    ProductName = Normalize(p.ProductName),
                    CategoryId = p.CategoryId,
                    CategoryName = Normalize(p.CategoryName),
                    Price = p.Price < 0 ? 0 : p.Price
                };
            })
            .ToList();

        var validCustomerIds = customers.Select(c => c.CustomerId).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var productById = products.ToDictionary(p => p.ProductId, p => p);
        var orderById = csvOrders
            .Where(o => o.OrderId > 0)
            .GroupBy(o => o.OrderId)
            .ToDictionary(g => g.Key, g => g.First());

        var factsByNaturalKey = new Dictionary<(int OrderId, int ProductId), FactSalesDto>();

        foreach (var record in historicalRecords)
        {
            if (record.OrderId <= 0 || record.ProductId <= 0 || record.Quantity <= 0 || record.OrderDate == DateTime.MinValue)
            {
                continue;
            }

            var customerId = Normalize(record.CustomerId);
            if (!validCustomerIds.Contains(customerId))
            {
                continue;
            }

            var status = NormalizeStatus(record.Status);
            if (string.IsNullOrWhiteSpace(status))
            {
                continue;
            }

            var unitPrice = record.UnitPrice > 0
                ? record.UnitPrice
                : record.Quantity > 0 ? decimal.Round(record.TotalPrice / record.Quantity, 2) : 0;

            if (unitPrice <= 0)
            {
                continue;
            }

            var total = decimal.Round(record.Quantity * unitPrice, 2);
            var dateKey = BuildDateKey(record.OrderDate.Date);

            factsByNaturalKey[(record.OrderId, record.ProductId)] = new FactSalesDto
            {
                DateKey = dateKey,
                CustomerId = customerId,
                ProductId = record.ProductId,
                StatusName = status,
                OrderId = record.OrderId,
                Quantity = record.Quantity,
                UnitPrice = unitPrice,
                TotalPrice = total,
                Discount = 0
            };
        }

        foreach (var detail in csvOrderDetails)
        {
            if (detail.OrderId <= 0 || detail.ProductId <= 0 || detail.Quantity <= 0)
            {
                continue;
            }

            if (!orderById.TryGetValue(detail.OrderId, out var order))
            {
                continue;
            }

            var customerId = Normalize(order.CustomerId);
            if (!validCustomerIds.Contains(customerId))
            {
                continue;
            }

            if (!productById.TryGetValue(detail.ProductId, out var product))
            {
                continue;
            }

            var status = NormalizeStatus(order.Status);
            if (string.IsNullOrWhiteSpace(status))
            {
                continue;
            }

            var unitPrice = product.Price > 0
                ? product.Price
                : detail.Quantity > 0 ? decimal.Round(detail.TotalPrice / detail.Quantity, 2) : 0;

            if (unitPrice <= 0 || order.OrderDate == DateTime.MinValue)
            {
                continue;
            }

            var total = decimal.Round(detail.Quantity * unitPrice, 2);
            var dateKey = BuildDateKey(order.OrderDate.Date);
            var naturalKey = (detail.OrderId, detail.ProductId);

            if (!factsByNaturalKey.ContainsKey(naturalKey))
            {
                factsByNaturalKey[naturalKey] = new FactSalesDto
                {
                    DateKey = dateKey,
                    CustomerId = customerId,
                    ProductId = detail.ProductId,
                    StatusName = status,
                    OrderId = detail.OrderId,
                    Quantity = detail.Quantity,
                    UnitPrice = unitPrice,
                    TotalPrice = total,
                    Discount = 0
                };
            }
        }

        var facts = factsByNaturalKey.Values
            .Where(f => productById.ContainsKey(f.ProductId))
            .ToList();

        var statuses = facts
            .Select(f => f.StatusName)
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var dates = facts
            .Select(f => DateTime.ParseExact(f.DateKey.ToString(), "yyyyMMdd", CultureInfo.InvariantCulture))
            .Distinct()
            .Select(ToDimDate)
            .ToList();

        return new EtlPayloadDto
        {
            Dates = dates,
            Customers = customers,
            Products = products,
            Statuses = statuses,
            Facts = facts
        };
    }

    private static DimDateDto ToDimDate(DateTime date)
    {
        var calendar = CultureInfo.InvariantCulture.Calendar;
        var week = calendar.GetWeekOfYear(date, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
        var quarter = ((date.Month - 1) / 3) + 1;

        return new DimDateDto
        {
            DateKey = BuildDateKey(date),
            FullDate = date,
            Year = date.Year,
            Quarter = quarter,
            QuarterName = $"Q{quarter}",
            Month = date.Month,
            MonthName = date.ToString("MMMM", CultureInfo.InvariantCulture),
            Week = week,
            Day = date.Day,
            DayOfWeekNum = (int)date.DayOfWeek,
            DayOfWeekName = date.DayOfWeek.ToString(),
            IsWeekend = date.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday
        };
    }

    private static int BuildDateKey(DateTime date) =>
        (date.Year * 10000) + (date.Month * 100) + date.Day;

    private static string Normalize(string? value) => (value ?? string.Empty).Trim();

    private static string NormalizeStatus(string? status)
    {
        var normalized = Normalize(status);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return string.Empty;
        }

        return CultureInfo.InvariantCulture.TextInfo.ToTitleCase(normalized.ToLowerInvariant());
    }
}
