using SalesAnalysisETL.Domain.Entities.API;

namespace SalesAnalysisETL.Application.Interfaces.Repositories;

public interface IApiExtractionRepository
{
    Task<(IReadOnlyCollection<ApiCustomer> Customers, IReadOnlyCollection<ApiProduct> Products)> ExtractAsync(CancellationToken cancellationToken);
}
