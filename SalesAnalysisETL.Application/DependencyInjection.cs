using Microsoft.Extensions.DependencyInjection;
using SalesAnalysisETL.Application.Interfaces.Services;
using SalesAnalysisETL.Application.Services;

namespace SalesAnalysisETL.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        services.AddScoped<IExtractionService, ExtractionService>();
        return services;
    }
}
