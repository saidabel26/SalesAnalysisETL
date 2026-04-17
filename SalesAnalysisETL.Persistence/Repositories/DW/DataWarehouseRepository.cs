using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SalesAnalysisETL.Application.DTOs;
using SalesAnalysisETL.Application.Interfaces.Repositories;

namespace SalesAnalysisETL.Persistence.Repositories.DW;

public class DataWarehouseRepository : IDataWarehouseRepository
{
    private readonly string _connectionString;

    public DataWarehouseRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("SalesAnalysisSystemDW")
            ?? throw new InvalidOperationException("No se encontró la cadena de conexión 'SalesAnalysisSystemDW'.");
    }

    public async Task<WarehouseLoadSummaryDto> LoadAsync(EtlPayloadDto payload, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var dbTransaction = await connection.BeginTransactionAsync(cancellationToken);
        var transaction = (SqlTransaction)dbTransaction;

        try
        {
            await ClearWarehouseAsync(connection, transaction, cancellationToken);

            var datesInserted = await BulkInsertDimDateAsync(connection, transaction, payload.Dates, cancellationToken);
            var customersInserted = await BulkInsertDimCustomerAsync(connection, transaction, payload.Customers, cancellationToken);
            var productsInserted = await BulkInsertDimProductAsync(connection, transaction, payload.Products, cancellationToken);
            var statusesInserted = await BulkInsertDimStatusAsync(connection, transaction, payload.Statuses, cancellationToken);
            var factsInserted = await BulkInsertFactSalesAsync(connection, transaction, payload.Facts, cancellationToken);

            await dbTransaction.CommitAsync(cancellationToken);

            return new WarehouseLoadSummaryDto
            {
                DatesInsertedCount = datesInserted,
                CustomersInsertedCount = customersInserted,
                CustomersUpdatedCount = 0,
                ProductsInsertedCount = productsInserted,
                ProductsUpdatedCount = 0,
                StatusesInsertedCount = statusesInserted,
                FactsInsertedCount = factsInserted
            };
        }
        catch
        {
            await dbTransaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    private static async Task ClearWarehouseAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        const string sql = @"
DELETE FROM FACT_Sales;
DELETE FROM DIM_OrderStatus;
DELETE FROM DIM_Product;
DELETE FROM DIM_Customer;
DELETE FROM DIM_Date;";

        await using var command = new SqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<int> BulkInsertDimDateAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<DimDateDto> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        var table = new DataTable();
        table.Columns.Add("DateKey", typeof(int));
        table.Columns.Add("FullDate", typeof(DateTime));
        table.Columns.Add("Year", typeof(int));
        table.Columns.Add("Quarter", typeof(int));
        table.Columns.Add("QuarterName", typeof(string));
        table.Columns.Add("Month", typeof(int));
        table.Columns.Add("MonthName", typeof(string));
        table.Columns.Add("Week", typeof(int));
        table.Columns.Add("Day", typeof(int));
        table.Columns.Add("DayOfWeekNum", typeof(int));
        table.Columns.Add("DayOfWeekName", typeof(string));
        table.Columns.Add("IsWeekend", typeof(bool));

        foreach (var row in rows)
        {
            table.Rows.Add(
                row.DateKey,
                row.FullDate.Date,
                row.Year,
                row.Quarter,
                row.QuarterName,
                row.Month,
                row.MonthName,
                row.Week,
                row.Day,
                row.DayOfWeekNum,
                row.DayOfWeekName,
                row.IsWeekend);
        }

        await BulkCopyAsync(connection, transaction, table, "DIM_Date", cancellationToken);
        return table.Rows.Count;
    }

    private static async Task<int> BulkInsertDimCustomerAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<DimCustomerDto> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        var table = new DataTable();
        table.Columns.Add("CustomerID", typeof(string));
        table.Columns.Add("FirstName", typeof(string));
        table.Columns.Add("LastName", typeof(string));
        table.Columns.Add("FullName", typeof(string));
        table.Columns.Add("Email", typeof(string));
        table.Columns.Add("Phone", typeof(string));
        table.Columns.Add("City", typeof(string));
        table.Columns.Add("Country", typeof(string));

        foreach (var row in rows)
        {
            table.Rows.Add(
                row.CustomerId,
                DbValue(row.FirstName),
                DbValue(row.LastName),
                DbValue(row.FullName),
                DbValue(row.Email),
                DbValue(row.Phone),
                DbValue(row.City),
                DbValue(row.Country));
        }

        await BulkCopyAsync(connection, transaction, table, "DIM_Customer", cancellationToken);
        return table.Rows.Count;
    }

    private static async Task<int> BulkInsertDimProductAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<DimProductDto> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        var table = new DataTable();
        table.Columns.Add("ProductID", typeof(int));
        table.Columns.Add("ProductName", typeof(string));
        table.Columns.Add("CategoryID", typeof(int));
        table.Columns.Add("CategoryName", typeof(string));
        table.Columns.Add("Price", typeof(decimal));

        foreach (var row in rows)
        {
            table.Rows.Add(row.ProductId, row.ProductName, row.CategoryId, row.CategoryName, row.Price);
        }

        await BulkCopyAsync(connection, transaction, table, "DIM_Product", cancellationToken);
        return table.Rows.Count;
    }

    private static async Task<int> BulkInsertDimStatusAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<string> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        var distinctStatuses = rows
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (distinctStatuses.Count == 0)
        {
            return 0;
        }

        var table = new DataTable();
        table.Columns.Add("StatusName", typeof(string));

        foreach (var status in distinctStatuses)
        {
            table.Rows.Add(status);
        }

        await BulkCopyAsync(connection, transaction, table, "DIM_OrderStatus", cancellationToken);
        return table.Rows.Count;
    }

    private static async Task<int> BulkInsertFactSalesAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<FactSalesDto> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        const string createTempTableSql = @"
CREATE TABLE #FactSalesStaging
(
    DateKey INT NOT NULL,
    CustomerID VARCHAR(20) NOT NULL,
    ProductID INT NOT NULL,
    StatusName VARCHAR(30) NOT NULL,
    OrderID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    TotalPrice DECIMAL(10,2) NOT NULL,
    Discount DECIMAL(10,2) NOT NULL
);";

        await using (var createTemp = new SqlCommand(createTempTableSql, connection, transaction))
        {
            await createTemp.ExecuteNonQueryAsync(cancellationToken);
        }

        var table = new DataTable();
        table.Columns.Add("DateKey", typeof(int));
        table.Columns.Add("CustomerID", typeof(string));
        table.Columns.Add("ProductID", typeof(int));
        table.Columns.Add("StatusName", typeof(string));
        table.Columns.Add("OrderID", typeof(int));
        table.Columns.Add("Quantity", typeof(int));
        table.Columns.Add("UnitPrice", typeof(decimal));
        table.Columns.Add("TotalPrice", typeof(decimal));
        table.Columns.Add("Discount", typeof(decimal));

        foreach (var row in rows)
        {
            table.Rows.Add(
                row.DateKey,
                row.CustomerId,
                row.ProductId,
                row.StatusName,
                row.OrderId,
                row.Quantity,
                row.UnitPrice,
                row.TotalPrice,
                row.Discount);
        }

        await BulkCopyAsync(connection, transaction, table, "#FactSalesStaging", cancellationToken);

        const string insertFactSql = @"
INSERT INTO FACT_Sales
(
    DateKey,
    CustomerKey,
    ProductKey,
    StatusKey,
    OrderID,
    Quantity,
    UnitPrice,
    TotalPrice,
    Discount
)
SELECT
    s.DateKey,
    c.CustomerKey,
    p.ProductKey,
    os.StatusKey,
    s.OrderID,
    s.Quantity,
    s.UnitPrice,
    s.TotalPrice,
    s.Discount
FROM #FactSalesStaging s
INNER JOIN DIM_Customer c ON c.CustomerID = s.CustomerID
INNER JOIN DIM_Product p ON p.ProductID = s.ProductID
INNER JOIN DIM_OrderStatus os ON os.StatusName = s.StatusName;";

        await using var insertFactCommand = new SqlCommand(insertFactSql, connection, transaction);
        return await insertFactCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task BulkCopyAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        DataTable table,
        string destinationTable,
        CancellationToken cancellationToken)
    {
        using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.CheckConstraints, transaction)
        {
            DestinationTableName = destinationTable,
            BatchSize = 5000,
            BulkCopyTimeout = 120
        };

        foreach (DataColumn column in table.Columns)
        {
            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
        }

        await bulkCopy.WriteToServerAsync(table, cancellationToken);
    }

    private static object DbValue(string value) =>
        string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
}
