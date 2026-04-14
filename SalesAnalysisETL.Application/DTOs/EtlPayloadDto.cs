namespace SalesAnalysisETL.Application.DTOs;

public class EtlPayloadDto
{
    public IReadOnlyCollection<DimDateDto> Dates { get; init; } = [];
    public IReadOnlyCollection<DimCustomerDto> Customers { get; init; } = [];
    public IReadOnlyCollection<DimProductDto> Products { get; init; } = [];
    public IReadOnlyCollection<string> Statuses { get; init; } = [];
    public IReadOnlyCollection<FactSalesDto> Facts { get; init; } = [];
}

public class DimDateDto
{
    public int DateKey { get; init; }
    public DateTime FullDate { get; init; }
    public int Year { get; init; }
    public int Quarter { get; init; }
    public string QuarterName { get; init; } = string.Empty;
    public int Month { get; init; }
    public string MonthName { get; init; } = string.Empty;
    public int Week { get; init; }
    public int Day { get; init; }
    public int DayOfWeekNum { get; init; }
    public string DayOfWeekName { get; init; } = string.Empty;
    public bool IsWeekend { get; init; }
}

public class DimCustomerDto
{
    public string CustomerId { get; init; } = string.Empty;
    public string FirstName { get; init; } = string.Empty;
    public string LastName { get; init; } = string.Empty;
    public string FullName { get; init; } = string.Empty;
    public string Email { get; init; } = string.Empty;
    public string Phone { get; init; } = string.Empty;
    public string City { get; init; } = string.Empty;
    public string Country { get; init; } = string.Empty;
}

public class DimProductDto
{
    public int ProductId { get; init; }
    public string ProductName { get; init; } = string.Empty;
    public int CategoryId { get; init; }
    public string CategoryName { get; init; } = string.Empty;
    public decimal Price { get; init; }
}

public class FactSalesDto
{
    public int DateKey { get; init; }
    public string CustomerId { get; init; } = string.Empty;
    public int ProductId { get; init; }
    public string StatusName { get; init; } = string.Empty;
    public int OrderId { get; init; }
    public int Quantity { get; init; }
    public decimal UnitPrice { get; init; }
    public decimal TotalPrice { get; init; }
    public decimal Discount { get; init; }
}

public class WarehouseLoadSummaryDto
{
    public int DatesInsertedCount { get; init; }
    public int CustomersInsertedCount { get; init; }
    public int CustomersUpdatedCount { get; init; }
    public int ProductsInsertedCount { get; init; }
    public int ProductsUpdatedCount { get; init; }
    public int StatusesInsertedCount { get; init; }
    public int FactsInsertedCount { get; init; }
}
