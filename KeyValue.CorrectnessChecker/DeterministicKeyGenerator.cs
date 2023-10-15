public class DeterministicKeyGenerator
{
    public static List<TradeKey> Generate(int count)
    {
        var keys = new List<TradeKey>(count);

        for (var i = 0; i < count; i++)
        {
            var key = new TradeKey
            {
                TradeDate = new DateOnly(2021, 1, 1),
                ExchangeTradeId = i.ToString(),
                ExchangeLinkId = i.ToString(),
            };

            keys.Add(key);
        }

        return keys;
    }
}
