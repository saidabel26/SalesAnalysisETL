using SalesAnalysisETL.API.Data.Entities;

namespace SalesAnalysisETL.API.Data.Interface;

public interface IProductRepository
{
    Task<IReadOnlyCollection<ProductEntity>> GetAllAsync(CancellationToken cancellationToken);
}
