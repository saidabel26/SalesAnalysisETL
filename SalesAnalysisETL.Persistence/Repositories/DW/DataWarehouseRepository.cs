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
            var datesInserted = await LoadDimDateAsync(connection, transaction, payload.Dates, cancellationToken);
            var (customersInserted, customersUpdated) = await LoadDimCustomerAsync(connection, transaction, payload.Customers, cancellationToken);
            var (productsInserted, productsUpdated) = await LoadDimProductAsync(connection, transaction, payload.Products, cancellationToken);
            var statusesInserted = await LoadDimStatusAsync(connection, transaction, payload.Statuses, cancellationToken);

            var customerKeyMap = await GetCustomerKeysAsync(connection, transaction, payload.Customers.Select(c => c.CustomerId), cancellationToken);
            var productKeyMap = await GetProductKeysAsync(connection, transaction, payload.Products.Select(p => p.ProductId), cancellationToken);
            var statusKeyMap = await GetStatusKeysAsync(connection, transaction, payload.Statuses, cancellationToken);

            var factsInserted = await LoadFactSalesAsync(
                connection,
                transaction,
                payload.Facts,
                customerKeyMap,
                productKeyMap,
                statusKeyMap,
                cancellationToken);

            await dbTransaction.CommitAsync(cancellationToken);

            return new WarehouseLoadSummaryDto
            {
                DatesInsertedCount = datesInserted,
                CustomersInsertedCount = customersInserted,
                CustomersUpdatedCount = customersUpdated,
                ProductsInsertedCount = productsInserted,
                ProductsUpdatedCount = productsUpdated,
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

    private static async Task<int> LoadDimDateAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<DimDateDto> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        const string sql = @"
IF NOT EXISTS (SELECT 1 FROM DIM_Date WHERE DateKey = @DateKey)
BEGIN
    INSERT INTO DIM_Date
    (
        DateKey,
        FullDate,
        Year,
        Quarter,
        QuarterName,
        Month,
        MonthName,
        Week,
        Day,
        DayOfWeekNum,
        DayOfWeekName,
        IsWeekend
    )
    VALUES
    (
        @DateKey,
        @FullDate,
        @Year,
        @Quarter,
        @QuarterName,
        @Month,
        @MonthName,
        @Week,
        @Day,
        @DayOfWeekNum,
        @DayOfWeekName,
        @IsWeekend
    );
    SELECT 1;
END
ELSE
BEGIN
    SELECT 0;
END";

        var inserted = 0;

        foreach (var row in rows)
        {
            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@DateKey", row.DateKey);
            command.Parameters.AddWithValue("@FullDate", row.FullDate.Date);
            command.Parameters.AddWithValue("@Year", row.Year);
            command.Parameters.AddWithValue("@Quarter", row.Quarter);
            command.Parameters.AddWithValue("@QuarterName", row.QuarterName);
            command.Parameters.AddWithValue("@Month", row.Month);
            command.Parameters.AddWithValue("@MonthName", row.MonthName);
            command.Parameters.AddWithValue("@Week", row.Week);
            command.Parameters.AddWithValue("@Day", row.Day);
            command.Parameters.AddWithValue("@DayOfWeekNum", row.DayOfWeekNum);
            command.Parameters.AddWithValue("@DayOfWeekName", row.DayOfWeekName);
            command.Parameters.AddWithValue("@IsWeekend", row.IsWeekend);

            var result = (int)(await command.ExecuteScalarAsync(cancellationToken) ?? 0);
            inserted += result;
        }

        return inserted;
    }

    private static async Task<(int Inserted, int Updated)> LoadDimCustomerAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<DimCustomerDto> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return (0, 0);
        }

        const string existsSql = "SELECT COUNT(1) FROM DIM_Customer WHERE CustomerID = @CustomerID;";
        const string insertSql = @"
INSERT INTO DIM_Customer (CustomerID, FirstName, LastName, FullName, Email, Phone, City, Country)
VALUES (@CustomerID, @FirstName, @LastName, @FullName, @Email, @Phone, @City, @Country);";
        const string updateSql = @"
UPDATE DIM_Customer
SET FirstName = @FirstName,
    LastName = @LastName,
    FullName = @FullName,
    Email = @Email,
    Phone = @Phone,
    City = @City,
    Country = @Country,
    LoadDate = GETDATE()
WHERE CustomerID = @CustomerID;";

        var inserted = 0;
        var updated = 0;

        foreach (var row in rows)
        {
            var exists = await ExistsAsync(connection, transaction, existsSql, "@CustomerID", row.CustomerId, cancellationToken);

            await using var command = new SqlCommand(exists ? updateSql : insertSql, connection, transaction);
            command.Parameters.AddWithValue("@CustomerID", row.CustomerId);
            command.Parameters.AddWithValue("@FirstName", NullableString(row.FirstName));
            command.Parameters.AddWithValue("@LastName", NullableString(row.LastName));
            command.Parameters.AddWithValue("@FullName", NullableString(row.FullName));
            command.Parameters.AddWithValue("@Email", NullableString(row.Email));
            command.Parameters.AddWithValue("@Phone", NullableString(row.Phone));
            command.Parameters.AddWithValue("@City", NullableString(row.City));
            command.Parameters.AddWithValue("@Country", NullableString(row.Country));
            await command.ExecuteNonQueryAsync(cancellationToken);

            if (exists)
            {
                updated++;
            }
            else
            {
                inserted++;
            }
        }

        return (inserted, updated);
    }

    private static async Task<(int Inserted, int Updated)> LoadDimProductAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<DimProductDto> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return (0, 0);
        }

        const string existsSql = "SELECT COUNT(1) FROM DIM_Product WHERE ProductID = @ProductID;";
        const string insertSql = @"
INSERT INTO DIM_Product (ProductID, ProductName, CategoryID, CategoryName, Price)
VALUES (@ProductID, @ProductName, @CategoryID, @CategoryName, @Price);";
        const string updateSql = @"
UPDATE DIM_Product
SET ProductName = @ProductName,
    CategoryID = @CategoryID,
    CategoryName = @CategoryName,
    Price = @Price,
    LoadDate = GETDATE()
WHERE ProductID = @ProductID;";

        var inserted = 0;
        var updated = 0;

        foreach (var row in rows)
        {
            var exists = await ExistsAsync(connection, transaction, existsSql, "@ProductID", row.ProductId, cancellationToken);

            await using var command = new SqlCommand(exists ? updateSql : insertSql, connection, transaction);
            command.Parameters.AddWithValue("@ProductID", row.ProductId);
            command.Parameters.AddWithValue("@ProductName", row.ProductName);
            command.Parameters.AddWithValue("@CategoryID", row.CategoryId);
            command.Parameters.AddWithValue("@CategoryName", row.CategoryName);
            command.Parameters.AddWithValue("@Price", row.Price);
            await command.ExecuteNonQueryAsync(cancellationToken);

            if (exists)
            {
                updated++;
            }
            else
            {
                inserted++;
            }
        }

        return (inserted, updated);
    }

    private static async Task<int> LoadDimStatusAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<string> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        const string sql = @"
IF NOT EXISTS (SELECT 1 FROM DIM_OrderStatus WHERE StatusName = @StatusName)
BEGIN
    INSERT INTO DIM_OrderStatus (StatusName)
    VALUES (@StatusName);
    SELECT 1;
END
ELSE
BEGIN
    SELECT 0;
END";

        var inserted = 0;

        foreach (var status in rows)
        {
            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@StatusName", status);
            var result = (int)(await command.ExecuteScalarAsync(cancellationToken) ?? 0);
            inserted += result;
        }

        return inserted;
    }

    private static async Task<int> LoadFactSalesAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyCollection<FactSalesDto> rows,
        IReadOnlyDictionary<string, int> customerKeys,
        IReadOnlyDictionary<int, int> productKeys,
        IReadOnlyDictionary<string, int> statusKeys,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        const string sql = @"
IF NOT EXISTS
(
    SELECT 1
    FROM FACT_Sales f
    WHERE f.OrderID = @OrderID
      AND f.ProductKey = @ProductKey
)
BEGIN
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
    VALUES
    (
        @DateKey,
        @CustomerKey,
        @ProductKey,
        @StatusKey,
        @OrderID,
        @Quantity,
        @UnitPrice,
        @TotalPrice,
        @Discount
    );
    SELECT 1;
END
ELSE
BEGIN
    SELECT 0;
END";

        var inserted = 0;

        foreach (var row in rows)
        {
            if (!customerKeys.TryGetValue(row.CustomerId, out var customerKey) ||
                !productKeys.TryGetValue(row.ProductId, out var productKey) ||
                !statusKeys.TryGetValue(row.StatusName, out var statusKey))
            {
                continue;
            }

            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@DateKey", row.DateKey);
            command.Parameters.AddWithValue("@CustomerKey", customerKey);
            command.Parameters.AddWithValue("@ProductKey", productKey);
            command.Parameters.AddWithValue("@StatusKey", statusKey);
            command.Parameters.AddWithValue("@OrderID", row.OrderId);
            command.Parameters.AddWithValue("@Quantity", row.Quantity);
            command.Parameters.AddWithValue("@UnitPrice", row.UnitPrice);
            command.Parameters.AddWithValue("@TotalPrice", row.TotalPrice);
            command.Parameters.AddWithValue("@Discount", row.Discount);

            var result = (int)(await command.ExecuteScalarAsync(cancellationToken) ?? 0);
            inserted += result;
        }

        return inserted;
    }

    private static async Task<bool> ExistsAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string sql,
        string parameterName,
        object parameterValue,
        CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue(parameterName, parameterValue);
        var result = (int)(await command.ExecuteScalarAsync(cancellationToken) ?? 0);
        return result > 0;
    }

    private static async Task<IReadOnlyDictionary<string, int>> GetCustomerKeysAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IEnumerable<string> customerIds,
        CancellationToken cancellationToken)
    {
        var keys = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var customerId in customerIds.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            const string sql = "SELECT CustomerKey FROM DIM_Customer WHERE CustomerID = @CustomerID;";
            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@CustomerID", customerId);
            var value = await command.ExecuteScalarAsync(cancellationToken);
            if (value is int key)
            {
                keys[customerId] = key;
            }
        }

        return keys;
    }

    private static async Task<IReadOnlyDictionary<int, int>> GetProductKeysAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IEnumerable<int> productIds,
        CancellationToken cancellationToken)
    {
        var keys = new Dictionary<int, int>();

        foreach (var productId in productIds.Distinct())
        {
            const string sql = "SELECT ProductKey FROM DIM_Product WHERE ProductID = @ProductID;";
            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@ProductID", productId);
            var value = await command.ExecuteScalarAsync(cancellationToken);
            if (value is int key)
            {
                keys[productId] = key;
            }
        }

        return keys;
    }

    private static async Task<IReadOnlyDictionary<string, int>> GetStatusKeysAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IEnumerable<string> statuses,
        CancellationToken cancellationToken)
    {
        var keys = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var status in statuses.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            const string sql = "SELECT StatusKey FROM DIM_OrderStatus WHERE StatusName = @StatusName;";
            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@StatusName", status);
            var value = await command.ExecuteScalarAsync(cancellationToken);
            if (value is int key)
            {
                keys[status] = key;
            }
        }

        return keys;
    }

    private static object NullableString(string value) =>
        string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
}
