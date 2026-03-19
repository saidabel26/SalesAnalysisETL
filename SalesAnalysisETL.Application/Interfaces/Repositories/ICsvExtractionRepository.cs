using SalesAnalysisETL.Domain.Entities.CSV;

namespace SalesAnalysisETL.Application.Interfaces.Repositories;

public interface ICsvExtractionRepository
{
    Task<(IReadOnlyCollection<CsvOrder> Orders, IReadOnlyCollection<CsvOrderDetail> OrderDetails)> ExtractAsync(CancellationToken cancellationToken);
}
