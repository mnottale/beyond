

namespace Beyond
{
    public static class Utils
    {
        public static Error ErrorFromCode(Error.Types.ErrorCode erc)
        {
            var res = new Error();
            res.Code = erc;
            return res;
        }
    }
}