using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SalesAnalysisETL.Application.Interfaces.Repositories;
using SalesAnalysisETL.Persistence.Repositories.API;
using SalesAnalysisETL.Persistence.Repositories.CSV;
using SalesAnalysisETL.Persistence.Repositories.DB;

namespace SalesAnalysisETL.Persistence;

public static class DependencyInjection
{
    public static IServiceCollection AddPersistence(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<CsvSourceOptions>(configuration.GetSection(CsvSourceOptions.SectionName));
        services.Configure<ApiSourceOptions>(configuration.GetSection(ApiSourceOptions.SectionName));

        services.AddScoped<ICsvExtractionRepository, CsvExtractionRepository>();
        services.AddScoped<IHistoricalSalesRepository, HistoricalSalesRepository>();

        services.AddHttpClient<IApiExtractionRepository, ApiExtractionRepository>((serviceProvider, client) =>
        {
            var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<ApiSourceOptions>>().Value;
            if (string.IsNullOrWhiteSpace(options.BaseUrl))
            {
                throw new InvalidOperationException("La URL base del API no fue configurada en DataSources:Api:BaseUrl.");
            }

            client.BaseAddress = new Uri(options.BaseUrl);
        });

        return services;
    }
}
