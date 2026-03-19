using System.Globalization;
using Microsoft.Extensions.Options;
using SalesAnalysisETL.Application.Interfaces.Repositories;
using SalesAnalysisETL.Domain.Entities.CSV;

namespace SalesAnalysisETL.Persistence.Repositories.CSV;

public class CsvExtractionRepository : ICsvExtractionRepository
{
    private readonly CsvSourceOptions _options;

    public CsvExtractionRepository(IOptions<CsvSourceOptions> options)
    {
        _options = options.Value;
    }

    public async Task<(IReadOnlyCollection<CsvOrder> Orders, IReadOnlyCollection<CsvOrderDetail> OrderDetails)> ExtractAsync(CancellationToken cancellationToken)
    {
        var orders = await ReadOrdersAsync(_options.OrdersFilePath, cancellationToken);
        var orderDetails = await ReadOrderDetailsAsync(_options.OrderDetailsFilePath, cancellationToken);

        return (orders, orderDetails);
    }

    private static async Task<IReadOnlyCollection<CsvOrder>> ReadOrdersAsync(string path, CancellationToken cancellationToken)
    {
        ValidatePath(path);

        var results = new List<CsvOrder>();
        using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        _ = await reader.ReadLineAsync(cancellationToken);

        while (!reader.EndOfStream)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var line = await reader.ReadLineAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var values = line.Split(',', StringSplitOptions.TrimEntries);
            if (values.Length < 4)
            {
                continue;
            }

            if (!int.TryParse(values[0], out var orderId) ||
                !DateTime.TryParse(values[2], CultureInfo.InvariantCulture, DateTimeStyles.None, out var orderDate))
            {
                continue;
            }

            results.Add(new CsvOrder
            {
                OrderId = orderId,
                CustomerId = values[1],
                OrderDate = orderDate,
                Status = values[3]
            });
        }

        return results;
    }

    private static async Task<IReadOnlyCollection<CsvOrderDetail>> ReadOrderDetailsAsync(string path, CancellationToken cancellationToken)
    {
        ValidatePath(path);

        var results = new List<CsvOrderDetail>();
        using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        _ = await reader.ReadLineAsync(cancellationToken);

        while (!reader.EndOfStream)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var line = await reader.ReadLineAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var values = line.Split(',', StringSplitOptions.TrimEntries);
            if (values.Length < 4)
            {
                continue;
            }

            if (!int.TryParse(values[0], out var orderId) ||
                !int.TryParse(values[1], out var productId) ||
                !int.TryParse(values[2], out var quantity) ||
                !decimal.TryParse(values[3], CultureInfo.InvariantCulture, out var totalPrice))
            {
                continue;
            }

            results.Add(new CsvOrderDetail
            {
                OrderId = orderId,
                ProductId = productId,
                Quantity = quantity,
                TotalPrice = totalPrice
            });
        }

        return results;
    }

    private static void ValidatePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new InvalidOperationException("La ruta del archivo CSV no fue configurada.");
        }

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"No se encontró el archivo CSV configurado: {path}", path);
        }
    }
}
