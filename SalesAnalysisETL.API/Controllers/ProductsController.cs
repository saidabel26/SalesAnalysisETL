using Microsoft.AspNetCore.Mvc;
using SalesAnalysisETL.API.Data.Interface;

namespace SalesAnalysisETL.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ProductsController : ControllerBase
{
    private readonly IProductRepository _productRepository;
    private readonly ILogger<ProductsController> _logger;

    public ProductsController(IProductRepository productRepository, ILogger<ProductsController> logger)
    {
        _productRepository = productRepository;
        _logger = logger;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Solicitud recibida para obtener productos desde SalesAnalysisSystemDB.");
        var products = await _productRepository.GetAllAsync(cancellationToken);
        return Ok(products);
    }
}
