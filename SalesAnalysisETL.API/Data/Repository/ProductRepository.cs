using SalesAnalysisETL.API.Data.Context;
using SalesAnalysisETL.API.Data.Entities;
using SalesAnalysisETL.API.Data.Interface;

namespace SalesAnalysisETL.API.Data.Repository;

public class ProductRepository : IProductRepository
{
    private const string Query = @"
SELECT p.ProductID, p.ProductName, p.CategoryID, c.CategoryName, p.Price, p.Stock
FROM Products p
INNER JOIN Categories c ON c.CategoryID = p.CategoryID;";

    private readonly SalesAnalysisDbConnectionFactory _connectionFactory;

    public ProductRepository(SalesAnalysisDbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<IReadOnlyCollection<ProductEntity>> GetAllAsync(CancellationToken cancellationToken)
    {
        var products = new List<ProductEntity>();

        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = Query;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            products.Add(new ProductEntity
            {
                ProductId = reader.GetInt32(0),
                ProductName = reader.GetString(1),
                CategoryId = reader.GetInt32(2),
                CategoryName = reader.GetString(3),
                Price = reader.IsDBNull(4) ? 0 : reader.GetDecimal(4),
                Stock = reader.IsDBNull(5) ? 0 : reader.GetInt32(5)
            });
        }

        return products;
    }
}
