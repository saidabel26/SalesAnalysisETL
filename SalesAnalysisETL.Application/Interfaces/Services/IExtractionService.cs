using SalesAnalysisETL.Application.DTOs;

namespace SalesAnalysisETL.Application.Interfaces.Services;

public interface IExtractionService
{
    Task<ExtractionSummaryDto> RunExtractionAsync(CancellationToken cancellationToken);
}
