using Microsoft.Data.SqlClient;

namespace SalesAnalysisETL.API.Data.Context;

public class SalesAnalysisDbConnectionFactory
{
    private readonly string _connectionString;

    public SalesAnalysisDbConnectionFactory(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("SalesAnalysisSystemDB")
            ?? throw new InvalidOperationException("No se encontró la cadena de conexión 'SalesAnalysisSystemDB'.");
    }

    public SqlConnection CreateConnection() => new(_connectionString);
}
