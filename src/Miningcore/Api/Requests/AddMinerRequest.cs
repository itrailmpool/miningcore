namespace Miningcore.Api.Requests;

public class AddMinerRequest
{
    public MinerSettings Settings { get; set; }
}

public class MinerSettings
{
    public string PoolId { get; set; }
    public string WorkerName { get; set; }
    public string Password { get; set; }
    public string Address { get; set; }
    public decimal PaymentThreshold { get; set; }
}
