# aiida-akaikkr MCP 設計（v0.3.0、2026-09-24）

Claude（Desktop / Code）から AkaiKKR の計算を AiiDA 経由で投入し、状態と結果を読み、図と provenance を出すための MCP サーバの設計です。aiida-shotgun-csp の MCP（`mcp/server.py` → CLI を subprocess で呼ぶ）と同じ約束で作ります。実装済み（同日）。使い方は [mcp.md](mcp.md)。実装で設計から変えた点は末尾 §11。

## 0. 目的と範囲

- できること: 環境確認、CIF から構造パラメータの生成、go と後続モード（dos / spc31 / tc / j3.0 / fsm / cnd）の投入、状態確認、結果の要約、DOS / A(w,k) の図、参照値との比較、provenance graph。
- しないこと: verdi の任意実行、ノードの削除、計算機やコードの登録・変更（`verdi computer/code` は人が打つ）、同期で計算を待つこと。
- 対象環境: mygardenx2 の conda env `akaikkr`（AiiDA 2.9、プロファイル `akaikkr`、code `specx-akaikkr@mygardenx2-slurm` など）。リモート（他マシンの Claude Desktop）からは ssh ラッパーで同じ CLI を叩く。

## 1. 層

```
aiida_akaikkr/
  mcp/server.py      MCP（FastMCP）。aiida も pyakaikkr も import しない。`akaikkr-aiida <sub> --json` を subprocess で呼ぶだけ
  cli/main.py        argparse。SUBCOMMANDS 表からサブコマンドを組む。--json のとき stdout は JSON 1 個だけ
  cli/steps.py       投入系の実装（run_examples.py の build/split_param/make_common_param をここへ移す）。投入前の検証はここ
  query/             読み取り専用。プロセス一覧、results の要約、配列の取り出し、参照比較
  presets.py         物質プリセット（run_examples.py の MATERIALS / FSM_FROM_GO_POTENTIAL / CND_MATERIALS を移す）
  workflows/chain.py AkaikkrChainWorkChain（go → 後続モード）。1 回の submit で pk を 1 つ返すため
  plot.py            plot_results.py の関数化（ノード → PNG）
  logdir.py          行動の記録（~/.aiida-akaikkr/log/<kind>-<YYYY-MM>.jsonl）。aiida を import しない
  calculations/ parsers/   既存
```

`__init__.py` は遅延 import にし、`mcp/server.py` が aiida を読み込まない約束を守る。

## 2. 破ってはいけない約束

1. **MCP から verdi を実行させない。** `server.py` が起動できるのは `akaikkr-aiida` だけ（`ALLOWED_BINARY_NAMES`）で、サブコマンドは `SUBCOMMANDS` 由来の白名簿（`ALLOWED_SUBCOMMANDS`）に限る。`tests/test_no_arbitrary_verdi.py` で静的に見張る。
2. **MCP のブリッジは 60 秒で切れる。** 投入は `submit` して pk を返す。待つツールは `wait_seconds ≤ 45`。geometry モードの specx 実行（構造生成）は数秒なので同期で良いが、45 秒の timeout を付ける。
3. **`--json` のとき stdout は JSON 1 個だけ。** 失敗しても `{"ok": false, "error": "...", "hint": "..."}`。進捗と警告（AiiDA の deprecation など）は stderr。
4. **書き込みは旗で開く。** サーバ起動時の `--allow-submit` が無ければ投入系ツールは登録されない（一覧に出ない）。`--allow-control` が無ければ daemon 操作と kill は出ない。
5. **provenance に載る値と実際に使った値を食い違わせない。** CIF は `SinglefileData`、specx は `InstalledCode` を calcfunction の入力にする。パス文字列の `Str` は残さない。
6. **MCP のツール引数は必ず argv に渡す。** 引数を増やしたら `tests/test_mcp_argv.py`（全ツール × 全引数で `_build_argv` の結果に旗が含まれるか）に足す。shotgun-csp で `volume_scale` が静かに落ちた失敗の再発防止。
7. **説明は実物で裏を取ってから書く。** ツールの docstring に「〜は返さない」と書くなら、その場で grep する。
8. **行動を記録する。** 投入・kill・daemon 操作は `logdir.append_jsonl("action", {...caller: "mcp"|"cli"})`。何もしなかった呼び出しは書かない。書けなくても止めない。

## 3. ツール一覧

名前は `kkr_` で始める。列「旗」は登録に必要な起動オプション。

| ツール | 旗 | 引数 | 返すもの | CLI サブコマンド |
|---|---|---|---|---|
| kkr_status | — | | profile、storage、broker、daemon の各 ok、code 一覧 | status |
| kkr_codes | — | | code の label / computer / 実行ファイル / prepend の有無 | codes |
| kkr_computer_test | — | computer | `verdi computer test` 相当を Python API で（transport open、scheduler job 数、tmp file） | computer-test |
| kkr_daemon_status | — | | worker 数、pid | daemon-status |
| kkr_daemon_start / kkr_daemon_stop | control | workers | 実行結果。記録する | daemon-start / daemon-stop |
| kkr_structure_from_cif | submit | cif_path, code, preset?, displc, backend(cif/ase) | 共通パラメータ Dict の pk、type 一覧、a、brvtyp | structure |
| kkr_presets | — | | presets.py の物質名、CIF、後続モード、fspin、cnd の有無 | presets |
| kkr_submit_go | submit | structure_pk（Dict）or comp（単一サイト CPA 組成; polytyp, lattice, magtype）, code, parameters?（上書き dict）, displc, ncores, wallclock, label | go の pk、SLURM への投入は非同期 | submit-go |
| kkr_submit_followup | submit | go_pk, mode(dos/spc/tc/jij/fsm/cnd), fspin?, from_potential?, ncores, wallclock | 後続 CalcJob の pk | submit-followup |
| kkr_submit_chain | submit | structure_pk or preset / cif_path or comp（polytyp, lattice, magtype）, code, modes[], fspin?, spc_structure_pk?, parameters?（全ジョブへの上書き dict）, ncores, wallclock, label | AkaikkrChainWorkChain の pk | submit-chain |
| kkr_submit_preset | submit | preset（例 Fe）, code, displc, modes? | 同上（プリセットの共通パラメータで chain を投入） | submit-preset |
| kkr_process | — | pk | state、exit_status、exit_message、label、scheduler state、job id、入力ノード pk、出力ポート一覧、最後の report 3 行 | process |
| kkr_list | — | label_prefix?, days?, state?, limit | 直近のプロセス（pk、label、state、exit、作成時刻） | list |
| kkr_wait | — | pk, wait_seconds(≤45) | 終了したか、現在の state | wait |
| kkr_results | — | pk | results Dict の要約（te、moment、local_moment、type_of_site、conv、Tc、resis、cnd）と出力ポート | results |
| kkr_dos | — | pk, spin?, emin?, emax?, max_points | energy / dos の配列（間引き）と pdos の成分名・`nl_per_type` | dos |
| kkr_awk | — | pk, spin | klabel、kdist の節点、Awk の形（配列は返さず、`kkr_plot` を勧める） | awk |
| kkr_jij | — | pk, max_rows | Jij 表（distance、pair、J_ij）と Tc | jij |
| kkr_plot | — | dos_pk?, spc_pk?, outdir, prefix | 書いた PNG のパス | plot |
| kkr_compare_reference | — | pks[] or result_json, reference_json | te rdiff、moment、Tc、resis の表（compare_reference.py の関数化） | compare |
| kkr_provenance | — | pk, outdir, ancestor_depth, descendant_depth, format | graphviz で描いたファイルのパス | provenance |
| kkr_gotocomputer | — | pk | 作業ディレクトリのパスと主要ファイル名（`go.in`, `go.out` の先頭 5 行） | workdir |
| kkr_kill | control | pk | kill の結果。記録する | kill |

`kkr_process` の「最後の report」には、exit 321 のときに `go.out` の先頭（"illegal input" など）を添える。exit 321 の大半は specx が inputcard を拒否したものなので、それが一目で分かる形にする。

## 4. 返り値の形（主要なもの）

```json
// kkr_submit_go
{"ok": true, "pk": 2630, "label": "Cu_go", "code": "specx-akaikkr@mygardenx2-slurm",
 "inputs": {"structure_pk": 2619, "parameters_pk": 2621, "magtype": "nmag", "displc": false},
 "resources": {"num_cores_per_mpiproc": 8, "max_wallclock_seconds": 1800}}

// kkr_process
{"ok": true, "pk": 2630, "label": "Cu_go", "process_label": "specx_go", "state": "finished",
 "exit_status": 0, "exit_message": null, "scheduler_state": "done", "job_id": "181805",
 "outputs": ["potential", "remote_folder", "results", "retrieved", "structure"],
 "report_tail": ["could not parse scheduler output: ... (harmless: no slurmdbd)"]}

// kkr_results
{"ok": true, "pk": 2630, "go": "go", "total_energy": -3304.747105277, "total_moment": 0.0,
 "converged": true, "type_of_site": [{"type": "Cu", "components": ["Cu"]}],
 "local_moment": [0.0], "Tc": null, "resistivity": null, "conductivity": null}

// 失敗
{"ok": false, "error": "Node<12> is not a specx CalcJob (process_label=make_common_param)",
 "hint": "pass the pk of a specx_* CalcJob; see kkr_list"}
```

## 5. CLI

`[project.scripts] akaikkr-aiida = "aiida_akaikkr.cli.main:main"`。サブコマンドの表は 1 か所:

```python
SUBCOMMANDS = {
    "status": (query.status, READ), "codes": (query.codes, READ), ...
    "submit-go": (steps.submit_go, SUBMIT), "submit-chain": (steps.submit_chain, SUBMIT), ...
    "daemon-start": (control.daemon_start, CONTROL), "kill": (control.kill, CONTROL),
}
```

MCP はこの表の名前と種別（READ / SUBMIT / CONTROL）から白名簿と登録可否を組む。`--json` 無しでは人が読む表を出す。

`--profile` は省略時に既定プロファイル。`AIIDA_PATH` を尊重する。

## 6. AkaikkrChainWorkChain

run_examples.py の「go を待ってから後続を出す」を WorkChain にする。入力: `code`、`structure`（Dict）、`common`（Dict、split 前）、`modes`（List）、`fspin`、`fsm_from_potential`（Bool）、`displc`、`resources`。出力: `go`（CalcJob の各出力を expose）、`<mode>` ごとに namespace。失敗した go では後続を出さず exit code 400 で終える。spc の `structure_data` は go の `structure`（無ければ exit 401、Fe_lmd の扱いは入力 `spc_structure_from`（pk）で明示）。

これにより `kkr_submit_chain` は 1 回の submit で 1 つの pk を返し、60 秒の壁に当たらない。

## 7. 配線

- ローカル（mygardenx2 上の Claude Code）: `claude mcp add akaikkr -- /home/kino/miniforge3/envs/akaikkr/bin/akaikkr-mcp --allow-submit`
- Claude Desktop（別マシン）: `~/bin/akaikkr-mcp-remote.sh`（`ssh mygardenx2 /home/kino/miniforge3/envs/akaikkr/bin/akaikkr-mcp "$@"`）を `mcpServers` に登録。stdio を ssh 越しに通す。
- MCP と daemon は同じ env で動かす。プラグインを入れ直したら `verdi daemon restart` と MCP（Claude Desktop）の再起動の両方が要る。片方だけだと版が食い違う。

## 8. テスト

| 層 | 要るもの | 内容 |
|---|---|---|
| 1 | なし（aiida 不要） | `server.py` が aiida を import しないこと、`ALLOWED_SUBCOMMANDS == SUBCOMMANDS` の名前集合、ツール数 > 10、全ツールの全引数が argv に出ること、`--json` で失敗時も JSON 1 個 |
| 2 | aiida（sqlite の一時プロファイル） | CLI の READ 系が空 DB で `ok: true` を返す、`submit-go` が `--allow-submit` 無しで拒否される |
| 3 | 上 + specx（`AKAIKKR_PROGRAM_PATH`） | Cu の chain を localhost computer で run し、`kkr_results` が参照 te と一致 |

## 9. 実装順

1. `presets.py`、`cli/steps.py`（run_examples.py から移す。run_examples.py は薄いラッパーに）
2. `query/`、`cli/main.py`（`--json`）、READ 系 8 本
3. `workflows/chain.py` と SUBMIT 系
4. `mcp/server.py`、`logdir.py`、旗
5. テスト層 1 → 2 → 3、docs（`docs/mcp.md` に使い方、このファイルは設計として残す）
6. `pyproject.toml` に `mcp` extra（`mcp>=1.0`）と scripts、版を 0.3.0 に

## 10. 未決事項

- `kkr_dos` で配列をどこまで返すか（200 点 × spin なら JSON で問題ない。pdos は成分 × l で大きいので間引きと `max_points`）。
- 図の出力先の既定（提案: `~/aiida_work/figures/<pk>/`）。
- ASE calculator 経路（`pyakaikkr.ase.AkaiKKR`）を MCP に載せるか。AiiDA を通さないので provenance が無く、今回は載せない。
- gofmg の対応（fmg の CalcJob が要る）。

## 11. 実装メモ（2026-09-24）

- mcp 2.x では `FastMCP` が `mcp.server.mcpserver.MCPServer` になった。`server.add_tool(fn, name, description)` と `server.run("stdio")` を使う。
- `kkr_gotocomputer` は `kkr_workdir` に改名（サブコマンド `workdir`）。`kkr_submit_preset` は `kkr_submit_chain(preset=...)` に統合。`kkr_compare_reference` に `material`（ラベル無し・chain のジョブを `<material>_<mode>` に読み替える）を追加。
- ツール関数は明示的に 23 本書き、`TOOLS`（ツール名 → サブコマンド）で spec と結び付ける。テストが「全ツールの引数集合 == spec のオプション集合」と「全引数が argv に出る」を検査する。
- CLI は起動時に自分の env の bin を PATH の先頭に足す。MCP ホスト（Claude Desktop など）の PATH には `verdi` と `dot` が無いため。
- AiiDA のリンクマネージャ（`node.inputs` / `node.outputs`）は dict の API（`items`, `keys`, `get`）を持たず属性アクセスと解釈される。`query.nodes.input_nodes / output_nodes` で辞書にしてから使う。
- 行動の記録は `~/.aiida-akaikkr/log/action-<YYYY-MM>.jsonl`。`--caller`（cli / mcp）は CLI の隠しオプションで MCP が渡す。
- 実行確認: 読み取り 16 / submit 付き 20 / control 付き 23 ツールを stdio クライアントから列挙・呼び出し。Cu の `submit-chain`（go → dos, spc）が WorkChain として完走。
