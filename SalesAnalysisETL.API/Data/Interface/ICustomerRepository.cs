using SalesAnalysisETL.API.Data.Entities;

namespace SalesAnalysisETL.API.Data.Interface;

public interface ICustomerRepository
{
    Task<IReadOnlyCollection<CustomerEntity>> GetAllAsync(CancellationToken cancellationToken);
}
