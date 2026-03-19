using Microsoft.AspNetCore.Mvc;
using SalesAnalysisETL.API.Data.Interface;

namespace SalesAnalysisETL.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class CustomersController : ControllerBase
{
    private readonly ICustomerRepository _customerRepository;
    private readonly ILogger<CustomersController> _logger;

    public CustomersController(ICustomerRepository customerRepository, ILogger<CustomersController> logger)
    {
        _customerRepository = customerRepository;
        _logger = logger;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Solicitud recibida para obtener clientes desde SalesAnalysisSystemDB.");
        var customers = await _customerRepository.GetAllAsync(cancellationToken);
        return Ok(customers);
    }
}
