# aiida-akaikkr ドキュメント

AkaiKKR（specx）を AiiDA から実行するプラグインの説明です。2026-09-24 時点の実装（v0.2.0、AiiDA 2.9.2）に基づきます。

| 文書 | 内容 |
|---|---|
| [installation.md](installation.md) | 環境構築。AiiDA、RabbitMQ（conda、sudo 不要）、プロファイル、SLURM computer、specx code の登録 |
| [calcjobs.md](calcjobs.md) | CalcJob（go / fsm / dos / jij / tc / spc / cnd）の入力・出力・exit code、パーサーの挙動と制限 |
| [examples.md](examples.md) | `example/` のスクリプト（単発実行、テストセット一括実行、図の出力、参照値との比較、provenance graph） |
| [known_issues.md](known_issues.md) | 既知の問題と参照値との差 |

## 最小の流れ

```bash
conda activate akaikkr
verdi status                       # profile / broker / daemon が ✔ であること
cd aiida-akaikkr
python example/run_cu_go_dos_spc.py --code specx-akaikkr@mygardenx2-slurm
python example/plot_results.py --dos <dos pk> --spc <spc pk> --outdir example/figures --prefix Cu
```

## 関連

- 計算結果の解析は `pyakaikkr`（AkaiKKRPythonUtil/library/PyAkaiKKR）に依存します。構造パラメータの生成には `akaikkr_testscript`（同 library/AkaiKKRTestScript）を使います。
- AkaiKKRPythonUtil 側の文書は `AkaiKKRPythonUtil/docs/` にあります。
