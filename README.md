IB 接口对接样例
================

此项目主要是对 IB 接口进行对接，获取股票交易信息，并进行一定的计算。

由于 IB 无法处理大量的并发请求，样例代码有时会触发 IB 的接口错误：`cause - Only 50 simultaneous API historical data requests allowed`。

如果，需要进行大量数据的分析，并且注重时效性，则建议不要使用 IB 接口，而是选择一个专业的数据提供商。
