using SalesAnalysisETL.Application.DTOs;

namespace SalesAnalysisETL.Application.Interfaces.Repositories;

public interface IDataWarehouseRepository
{
    Task<WarehouseLoadSummaryDto> LoadAsync(EtlPayloadDto payload, CancellationToken cancellationToken);
}
