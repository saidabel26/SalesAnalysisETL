using SalesAnalysisETL.Domain.Entities.DB;

namespace SalesAnalysisETL.Application.Interfaces.Repositories;

public interface IHistoricalSalesRepository
{
    Task<IReadOnlyCollection<HistoricalSaleRecord>> ExtractAsync(CancellationToken cancellationToken);
}
