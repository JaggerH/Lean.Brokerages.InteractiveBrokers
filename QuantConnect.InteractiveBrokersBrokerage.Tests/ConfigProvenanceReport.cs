/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Configuration;

/// <summary>
/// 每次跑测试先说清楚：**这一轮用的到底是哪一份 config.json**。
///
/// `Config` 按进程工作目录解析 `config.json`，而测试的工作目录是 `bin/Debug/<tfm>/`——
/// 那是构建期拷进去的副本。改项目根目录那份而没重新构建，测试读到的仍是旧值，**没有任何
/// 报错**，表现为"配置写了却像没写"。排查 IB 网关时因此白跑过两轮，还据此得出过
/// 「`ib-tws-dir` 被忽略了」这个错误结论。
///
/// 这个 fixture 不去改构建方式（那要动 csproj，收益不抵风险），只把两份文件的路径和差异
/// 打出来：手快的人照样会改错文件，但他这一秒就看得见"我改的和我在测的不是同一份"。
///
/// 放在全局命名空间里，所以对整个程序集生效。**不打印任何凭据字段**——只打路径、字节数、
/// 内容是否一致，以及几个与账号无关的键。
/// </summary>
[SetUpFixture]
public class ConfigProvenanceReport
{
    private const string ConfigFileName = "config.json";

    /// <summary>与账号/密码无关、但真正影响测试行为的那几个键。</summary>
    private static readonly string[] BenignKeys =
    {
        "ib-tws-dir", "ib-version", "ib-port", "ib-host", "ib-trading-mode",
        "ib-enable-delayed-streaming-data"
    };

    [OneTimeSetUp]
    public void ReportWhichConfigIsActuallyInUse()
    {
        // Config 就是这么解析的：相对进程工作目录，没有公开的"告诉我你读的是哪个文件"接口，
        // 所以这里用同一条规则自己算一遍。
        var effective = Path.GetFullPath(ConfigFileName);
        var source = FindSourceConfig();

        TestContext.Progress.WriteLine($"[config] 生效的是   : {effective}"
            + (File.Exists(effective) ? $" ({new FileInfo(effective).Length} 字节)" : " (不存在)"));
        TestContext.Progress.WriteLine($"[config] 源文件在   : {source ?? "<没找到项目根目录下的 config.json>"}"
            + (source != null ? $" ({new FileInfo(source).Length} 字节)" : ""));

        foreach (var key in BenignKeys)
        {
            var value = Config.Get(key);
            if (!string.IsNullOrEmpty(value))
            {
                TestContext.Progress.WriteLine($"[config] {key} = {value}");
            }
        }

        if (source == null || !File.Exists(effective)
            || string.Equals(source, effective, StringComparison.Ordinal))
        {
            return;
        }

        if (File.ReadAllBytes(source).SequenceEqual(File.ReadAllBytes(effective)))
        {
            TestContext.Progress.WriteLine("[config] 两份一致。");
            return;
        }

        // 不一致就整个程序集停下来——这是"白跑两轮"的唯一预防：跑下去也只会得出对旧配置
        // 成立的结论，而那个结论看起来完全正常。停机的代价是重新构建一次，很便宜。
        Assert.Fail(
            $"config.json 有两份且内容不同：源文件 {source} 与测试实际读取的 {effective}。"
            + " 你改的是源文件，测试读的是 bin 里那份旧副本——先重新构建这个测试项目"
            + "（dotnet build QuantConnect.InteractiveBrokersBrokerage.Tests），再重跑。");
    }

    /// <summary>
    /// 从构建输出目录往上走，找到含有本测试项目 csproj 的那一层——项目根目录下的 config.json
    /// 就是人手改的那份。找不到就返回 null（别猜，别报错）。
    /// </summary>
    private static string FindSourceConfig()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            if (dir.GetFiles("*.csproj").Any(f => f.Name.Contains("InteractiveBrokersBrokerage.Tests")))
            {
                var candidate = Path.Combine(dir.FullName, ConfigFileName);
                return File.Exists(candidate) ? candidate : null;
            }
            dir = dir.Parent;
        }
        return null;
    }
}
