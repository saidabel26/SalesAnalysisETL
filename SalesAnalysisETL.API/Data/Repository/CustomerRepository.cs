using SalesAnalysisETL.API.Data.Context;
using SalesAnalysisETL.API.Data.Entities;
using SalesAnalysisETL.API.Data.Interface;

namespace SalesAnalysisETL.API.Data.Repository;

public class CustomerRepository : ICustomerRepository
{
    private const string Query = @"
SELECT CustomerID, FirstName, LastName, Email, Phone, City, Country
FROM Customers;";

    private readonly SalesAnalysisDbConnectionFactory _connectionFactory;

    public CustomerRepository(SalesAnalysisDbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<IReadOnlyCollection<CustomerEntity>> GetAllAsync(CancellationToken cancellationToken)
    {
        var customers = new List<CustomerEntity>();

        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = Query;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            customers.Add(new CustomerEntity
            {
                CustomerId = reader.GetString(0),
                FirstName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                LastName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                Email = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                Phone = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                City = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                Country = reader.IsDBNull(6) ? string.Empty : reader.GetString(6)
            });
        }

        return customers;
    }
}
