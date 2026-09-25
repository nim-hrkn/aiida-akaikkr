# aiida-akaikkr ドキュメント

AkaiKKR（specx）を AiiDA から実行するプラグインの説明です。2026-09-24 時点の実装（v0.3.0、AiiDA 2.9.2）に基づきます。

| 文書 | 内容 |
|---|---|
| [intro_ja.html](intro_ja.html) | 入門 | 初心者向けの説明（構成図、来歴の例、KKR の ewidth と semicore、GAES の 2 例、レポート、LLM 経由の操作、動かすまで）。`data/intro/build_intro.py` で SVG から組み立てる |
| [installation.md](installation.md) | 環境構築。AiiDA、RabbitMQ（conda、sudo 不要）、プロファイル、SLURM computer、specx code の登録 |
| [calcjobs.md](calcjobs.md) | CalcJob（go / fsm / dos / jij / tc / spc / cnd）の入力・出力・exit code、パーサーの挙動と制限 |
| [examples.md](examples.md) | `example/` のスクリプト（単発実行、テストセット一括実行、図の出力、参照値との比較、provenance graph） |
| [dos_ewidth_from_go_spec.md](dos_ewidth_from_go_spec.md) | 仕様書 | dos で go の ewidth を使う: 図の線は go の ewidth だけ（辿れなければ描かない）、dos の ewidth を go の ewidth から決める（既定 2·ewidth_go）、parser が begin_option と実効 mesh を results に残す（2026-09-25、未実装） |
| [report_spec.md](report_spec.md) | 仕様書（実装済み） | MCP / CLI から図と要約を 1 枚の HTML にする `report`（式、CIF / POSCAR / preset、空間群、全エネルギー、モーメント、電荷、DOS / A(w,k) / Jij / GAES の図、cnd）。図は PNG + SVG（A(w,k) のメッシュは rasterize）。pyakaikkr.plot / pyakaikkr.report と aiida_akaikkr.report の関数設計 |
| [gaes_workchain.md](gaes_workchain.md) | 使い方（テスト `tests/test_gaes.py`） | `AkaikkrGaesWorkChain`（go → dos → ギャップ判定 → 新 ewidth の反復で go の ewidth を自動決定）、CLI `submit-gaes` / `plot --gaes-pk`、MCP `kkr_submit_gaes` |
| [ewidth.md](ewidth.md) | go と dos の ewidth の意味の違い（go は [E_F − ewidth, E_F] を積分、dos は [E_F − ref·ewidth, E_F + (1 − ref)·ewidth] を描く）、ref のビルド差と `cemesr_ref=`、EW / EZ |
| [known_issues.md](known_issues.md) | 既知の問題と参照値との差 |
| [mcp.md](mcp.md) | CLI `akaikkr-aiida` と MCP サーバ `akaikkr-mcp` の使い方（v0.3.0）。配線、典型的な流れ、失敗の見方 |
| [mcp_design.md](mcp_design.md) | MCP サーバの設計。ツール一覧、約束、CLI、WorkChain、テスト層 |

## 最小の流れ

```bash
conda activate akaikkr
verdi status                       # profile / broker / daemon が ✔ であること
cd aiida-akaikkr
python example/run_cu_go_dos_spc.py --code specx-akaikkr@mygardenx2-slurm
akaikkr-aiida report --pk <chain / gaes / go pk> [--lang ja] [--embed png] [--outdir DIR]   # HTML レポート（MCP: kkr_report）
python example/plot_results.py --dos <dos pk> --spc <spc pk> [--jij <j3.0 pk>] --outdir example/figures --prefix Cu
```

## 関連

- 計算結果の解析は `pyakaikkr`（AkaiKKRPythonUtil/library/PyAkaiKKR）に依存します。構造パラメータの生成には `akaikkr_testscript`（同 library/AkaiKKRTestScript）を使います。
- AkaiKKRPythonUtil 側の文書は `AkaiKKRPythonUtil/docs/` にあります。
