# MCP と CLI の使い方（v0.3.0）

設計は [mcp_design.md](mcp_design.md)。ここは使い方です。

## 1. インストール

```bash
conda activate akaikkr
pip install -e "aiida-akaikkr[mcp,plot]"     # mcp>=2、matplotlib
verdi daemon restart                          # WorkChain の entry point (akaikkr.chain) を読み込ませる
akaikkr-aiida --json status                   # CLI
akaikkr-mcp --help                            # MCP サーバ
```

## 2. CLI `akaikkr-aiida`

```
akaikkr-aiida [--json] [--profile NAME] <subcommand> [--option value ...]
```

| 種別 | サブコマンド |
|---|---|
| read | status, codes, computer-test, daemon-status, presets, process, list, wait, results, dos, awk, jij, workdir, plot, compare, provenance |
| submit | structure, submit-go, submit-followup, submit-chain |
| control | daemon-start, daemon-stop, kill |

`--json` のとき stdout は JSON 1 個だけです。失敗も `{"ok": false, "error": ..., "hint": ...}` で返り、終了コードは 1。ライブラリの print（pymatgen の警告など）は stderr に回ります。

よく使う例:

```bash
akaikkr-aiida presets
akaikkr-aiida structure --preset Fe --code specx-akaikkr@mygardenx2-slurm          # 共通パラメータ Dict → pk
akaikkr-aiida submit-chain --preset Fe --code specx-akaikkr@mygardenx2-slurm      # go → fsm,tc,jij,dos,spc
akaikkr-aiida submit-chain --cif-path my.cif --modes dos,spc --code specx-akaikkr@mygardenx2-slurm
akaikkr-aiida process --pk 2805
akaikkr-aiida results --pk 2805                       # chain なら各モードの要約
akaikkr-aiida submit-followup --go-pk 2630 --mode tc
akaikkr-aiida dos --pk 2644 --emin -1 --emax 0.5 --max-points 100
akaikkr-aiida plot --dos-pk 2644 --spc-pk 2654 --outdir figures --prefix Cu
akaikkr-aiida compare --pks 2805 --reference-json ../AkaiKKRPythonUtil/tests/akaikkr/reference/ifort.json
akaikkr-aiida provenance --pk 2805 --fmt pdf
```

`--code` は環境変数 `AKAIKKR_CODE` でも指定できます。プリセットの CIF の場所は `AKAIKKR_STRUCTURE_DIR`（既定は `example/structure`）。

## 3. MCP サーバ `akaikkr-mcp`

```bash
akaikkr-mcp                       # 読み取りツールだけ（16 本）
akaikkr-mcp --allow-submit        # + structure / submit_go / submit_followup / submit_chain（20 本）
akaikkr-mcp --allow-submit --allow-control   # + daemon_start / daemon_stop / kill（23 本）
```

ツール名は `kkr_` + サブコマンド名（`kkr_status`, `kkr_submit_chain`, `kkr_compare_reference`, ...）。引数はサブコマンドのオプションと同じです。サーバは aiida を import せず、`akaikkr-aiida <sub> --json --caller mcp` を subprocess で呼びます（55 秒で打ち切り）。

### Claude Code（このマシン）

```bash
claude mcp add akaikkr -- /home/kino/miniforge3/envs/akaikkr/bin/akaikkr-mcp --allow-submit
```

### Claude Desktop（別マシンから ssh）

`~/bin/akaikkr-mcp-remote.sh`:
```bash
#!/bin/bash
exec ssh -o BatchMode=yes mygardenx2 /home/kino/miniforge3/envs/akaikkr/bin/akaikkr-mcp "$@"
```
`claude_desktop_config.json`:
```json
{"mcpServers": {"akaikkr": {"command": "/home/<user>/bin/akaikkr-mcp-remote.sh", "args": ["--allow-submit"]}}}
```

プラグインを入れ直したら `verdi daemon restart` と Claude Desktop の再起動の両方が要ります。

## 4. 典型的な流れ

1. `kkr_status` で profile / daemon / code を確認。daemon が止まっていれば `kkr_daemon_start`（control 旗が要る）か、人が `verdi daemon start`。
2. `kkr_presets` で物質を選ぶ、または CIF を用意する。
3. `kkr_submit_chain(preset="Fe", code="specx-akaikkr@mygardenx2-slurm")` → WorkChain の pk。
4. `kkr_wait(pk)` を数回、または `kkr_process(pk)`。`children` に go と後続の pk と状態が出る。
5. `kkr_results(pk)` で全エネルギー・モーメント・Tc、`kkr_plot(dos_pk, spc_pk)` で図、`kkr_compare_reference(pks="<pk>", reference_json=...)` で参照との比較、`kkr_provenance(pk)` でグラフ。

## 5. 失敗の見方

- `exit_status 321`（pot.dat が無い）: `kkr_process` の `stdout_head` に specx の先頭出力が入ります。"illegal input" なら inputcard が拒否されています（akaikkr_cnd で displc 無し、など）。
- WorkChain の exit 400 は go の失敗、401 は spc 用の構造が無い（lmd）、402 は後続の失敗、410 は未知のモード。`children` で該当ジョブを探し `kkr_process` で見ます。
- ツールが `did not finish within 55 s` を返したら、CLI 側は動いている可能性があります。`kkr_list` で確認してから再投入してください。
- daemon が止まっていると投入は受け付けられても進みません。`kkr_daemon_status` を見ます。

## 6. 行動の記録

投入・kill・daemon 操作は `~/.aiida-akaikkr/log/action-<YYYY-MM>.jsonl` に 1 行ずつ残ります（`AKAIKKR_LOG_DIR` で変更）。`caller` が `cli` / `mcp` のどちらかを示します。何もしなかった呼び出し（既に動いている daemon の start など）は書きません。

## 7. テスト

```bash
pytest tests            # aiida 不要（spec と MCP の構造、argv への引数の受け渡し）
```
