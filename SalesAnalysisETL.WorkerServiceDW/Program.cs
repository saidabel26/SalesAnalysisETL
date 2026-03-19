using SalesAnalysisETL.Application;
using SalesAnalysisETL.Persistence;
using SalesAnalysisETL.WorkerServiceDW;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddApplication()
    .AddPersistence(builder.Configuration);

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
