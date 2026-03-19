using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SalesAnalysisETL.Application.Interfaces.Repositories;
using SalesAnalysisETL.Domain.Entities.DB;

namespace SalesAnalysisETL.Persistence.Repositories.DB;

public class HistoricalSalesRepository : IHistoricalSalesRepository
{
    private const string Query = @"
SELECT
    od.OrderID,
    o.CustomerID,
    o.OrderDate,
    o.Status,
    od.ProductID,
    od.Quantity,
    p.Price AS UnitPrice,
    od.TotalPrice
FROM Orders o
INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
INNER JOIN Products p ON p.ProductID = od.ProductID;";

    private readonly string _connectionString;

    public HistoricalSalesRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("SalesAnalysisSystemDB")
            ?? throw new InvalidOperationException("No se encontró la cadena de conexión 'SalesAnalysisSystemDB'.");
    }

    public async Task<IReadOnlyCollection<HistoricalSaleRecord>> ExtractAsync(CancellationToken cancellationToken)
    {
        var records = new List<HistoricalSaleRecord>();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(Query, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            records.Add(new HistoricalSaleRecord
            {
                OrderId = reader.GetInt32(0),
                CustomerId = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                OrderDate = reader.IsDBNull(2) ? DateTime.MinValue : reader.GetDateTime(2),
                Status = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                ProductId = reader.GetInt32(4),
                Quantity = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                UnitPrice = reader.IsDBNull(6) ? 0 : reader.GetDecimal(6),
                TotalPrice = reader.IsDBNull(7) ? 0 : reader.GetDecimal(7)
            });
        }

        return records;
    }
}
