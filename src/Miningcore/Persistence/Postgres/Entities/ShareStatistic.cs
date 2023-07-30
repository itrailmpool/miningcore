namespace Miningcore.Persistence.Postgres.Entities;

public class ShareStatistic
{
    public string PoolId { get; set; }
    public long BlockHeight { get; set; }
    public string Miner { get; set; }
    public string Worker { get; set; }
    public string UserAgent { get; set; }
    public double Difficulty { get; set; }
    public double NetworkDifficulty { get; set; }
    public string IpAddress { get; set; }
    public string Source { get; set; }
    public DateTime Created { get; set; }
    public bool IsValid { get; set; }
    public string Device { get; set; }
}
