# dos で go の ewidth を使う（aiida-akaikkr）仕様

作成日: 2026-09-25
対象: aiida-akaikkr（`aiida_akaikkr/plot.py`, `presets.py`, `inputs.py`, `workflows/chain.py`, `cli/steps.py`, `parsers/akaikkr_parser.py`）、pyakaikkr（`DosPlotter`, `option` は 2026-09-25 に実装済み）
関連: [ewidth.md](ewidth.md)（go と dos の ewidth の意味の違い）、AkaiKKRPythonUtil/docs/dos_plot_ewidth_line.md、AkaiKKRPythonUtil/docs/option_access_usage.md

## 0. 目的

go の ewidth（SCF の積分路の下端 E_F − ewidth_go）は、dos の ewidth（DOS を描く窓の幅）とは別物である。dos に関わる場面で常に **go の ewidth** を基準にし、dos 自身の ewidth を go の代わりに使わないようにする。対象は 3 つ。

| # | 場面 | いま | 変更 |
|---|---|---|---|
| A | DOS / PDOS 図の一点鎖線 | go が provenance に無いとき dos 自身の ewidth に落として線を引く（`contour_bottom`、plot.py:46） | go の ewidth が取れないときは線を描かない。dos の ewidth は使わない |
| B | dos の窓（ewidth_dos） | `presets.mode_parameter_overrides("dos")` が一律 `ewidth=2.0` | go の ewidth から決める（既定 `ewidth_dos = 2·ewidth_go`）。明示の `overrides["ewidth"]` があればそれを優先 |
| C | provenance | `results` に `ewidth` はあるが、`begin_option`（`cemesr_ref` 等）と実効 mesh は無い | parser が `results["option"]`, `results["emesh"]` を入れ、図と B の判定が `cemesr_ref` を provenance から取れるようにする |

A はユーザーが 2026-09-25 に決めたもの（「go が辿れないときは ewidth 線を書かない」）。B と C は A を確実にするための変更で、B の既定値は本仕様の提案である。

## 1. A: 図の線は go の ewidth だけ（`plot.py`）

```python
def contour_bottom(node) -> tuple[float, str] | None
def _mark_contour_bottom(ax, energy, node) -> bool
```

- `contour_bottom` は `node.inputs.potential.creator`（go または fsm の CalcJob）の `outputs.results["ewidth"]` があれば `(-|ewidth_go|, label)`、無ければ **None**。dos ノード自身の `results["ewidth"]` は見ない。
- `_mark_contour_bottom` は None なら何も描かず False を返す。`plot_dos` / `plot_pdos` は False のとき凡例に「ewidth of go: unknown (potential from file)」の注記を 1 行入れる（線が無い理由が図から分かるように）。
- 線が mesh の外なら横軸を線まで広げる（現状どおり）。
- `docs/examples.md` §4 の「go が provenance に無いときは dos 自身の ewidth を使います」を「線を描きません」に直す。

## 2. B: dos の ewidth を go の ewidth から決める

### 2.1 規則

dos の窓は [E_F − ref·ewidth_dos, E_F + (1 − ref)·ewidth_dos]（ref: akaikkr 0.75、akaikkr_cnd 0.5、`option={"cemesr_ref": ...}` で変更）。E_F − ewidth_go を窓の中に、下側に余裕をもって入れるために

```
ewidth_dos = dos_ewidth_scale × ewidth_go        （dos_ewidth_scale の既定 2.0）
```

とする。既定では akaikkr の窓が [−1.5·ewidth_go, +0.5·ewidth_go]、akaikkr_cnd の窓が [−ewidth_go, +ewidth_go] になる。akaikkr_cnd では下側の余裕が無いので、`option={"cemesr_ref": 0.75}` を dos に渡すか scale を上げる（§2.3 の警告）。

- 現行の例（go 1.0 → dos 2.0 の Cu, Fe, Co, Ni, NiFe, Fe_lmd, FeRh05Pt05）は同じ値になる。go 1.2〜1.7 の物質（Co2MnSi, AlMnFeCo_bcc, FeB195, GaAs, SmCo5_oc）は dos が 2.4〜3.4 になり、これまで窓の外だった E_F − ewidth_go が窓に入る。
- 刻みは `mse=201` 固定なので、scale を上げると粗くなる。E_F 付近を細かく見たいときは別の dos（`overrides={"ewidth": 1.0}`）を流す。

### 2.2 実装箇所

- `presets.mode_parameter_overrides(mode, fspin, from_potential, *, ewidth_go=None, dos_ewidth_scale=2.0)`: `mode == "dos"` のとき `ewidth_go` が与えられれば `{"record": "2nd", "ewidth": dos_ewidth_scale * ewidth_go}`、無ければ現行の 2.0（後方互換。`ewidth_go` を渡さない呼び出しは非推奨とし、ログに出す）。
- `inputs.split_param` / `build_calcjob` に `ewidth_go: float | None` と `dos_ewidth_scale` を追加。`overrides["ewidth"]` が明示されていればそれが勝つ（順序: mode 既定 → 明示 overrides、現行どおり）。
- go の ewidth の取り方（優先順）: (1) `go.outputs.results["ewidth"]`（出力ヘッダの値、specx が実際に使ったもの）、(2) `go.inputs.parameters["ewidth"]`。
  - `workflows/chain.py::run_followups`: `self.ctx.go` から取る。`spec.input("dos_ewidth_scale", Float, default 2.0)` を追加。
  - `cli/steps.py::submit_followup`: `go` ノードから取る。CLI / MCP の `submit-followup`, `submit-chain` に `--dos-ewidth-scale`（float、省略可）を足す（`cli/spec.py` の options に 1 行。MCP は spec から自動で生える）。
- dos 以外の followup（spc, tc, jij, fsm, cnd）は変えない（cnd の `ewidth=0.01` は別物）。

### 2.3 検査

- 提出前に `ref·ewidth_dos ≥ ewidth_go` を確かめ、満たさなければ `self.report` / CLI の warning に「E_F − ewidth_go は dos の窓の外」と出す（止めない）。ref は `overrides["option"]["cemesr_ref"]` があればその値、無ければ code のラベルに `cnd` を含めば 0.5、他は 0.75。
- 提出後の検査は C の `results["emesh"]` と DOS の mesh（`outputs.dos` の energy の最小値）で行い、`plot_dos` は線が mesh の外なら xlim を広げる（現状どおり）。

## 3. C: `begin_option` と実効 mesh を provenance に残す（parser）

- `parsers/akaikkr_parser.py` の共通部に `results["option"] = job.get_option(output_card)`（specx が使った option、echo 行から。無ければ `{}`）と `results["emesh"] = job.get_emesh_param(output_card)`（`{"meshr","mse","ng","mxl"}`）を追加する。`get_emesh_param` が `KKRValueAquisitionError` を上げる出力（途中で落ちたもの）では `None`。
- `results["cemesr_ref"]`: dos / spc の出力では `results["option"].get("cemesr_ref")`、無ければ code に応じた既定（0.75 / 0.5）を書く。図（§1）と §2.3 の検査はこれを使う。
- 入力側: `parameters` Dict に `option` を入れると `AkaikkrJob.make_inputcard` がそのまま書く（既に動く）。`build_calcjob` は `overrides["option"]` を `pyakaikkr.option.normalize_option(..., strict=True)` に通してから Dict に入れ、未知キーは提出前に `ValueError` にする。
- `docs/calcjobs.md` の `parameters` の行に `option`（`begin_option` の dict）を追記。`docs/ewidth.md` の「example の値」節を §2 の規則に合わせて書き直す。

## 4. テスト

- `tests/test_plot_contour.py`（aiida 不要、`node` を `SimpleNamespace` で偽装）: go が辿れる → `(-1.0, label)`、`potential` 入力が無い → None、`potential.creator` に `results` が無い → None。`_mark_contour_bottom` が False のとき `ax.get_lines()` に一点鎖線が無い。
- `tests/test_presets_dos_ewidth.py`（aiida 不要）: `mode_parameter_overrides("dos", ewidth_go=1.3)` が `ewidth=2.6`、`dos_ewidth_scale=1.5` で 1.95、`ewidth_go=None` で 2.0。
- 既存 `tests/test_spec_and_mcp.py`: `submit-followup` / `submit-chain` の新オプションが argv に届くこと（`test_every_tool_argument_reaches_argv` が自動で拾う）。
- AiiDA プロファイルがあるとき（`AIIDA_PROFILE=akaikkr`、SLURM）: Cu preset を `submit-chain --modes dos` で流し、dos の `inputs.parameters["ewidth"] == 2.0`、`results["emesh"]["mse"] == 201`、`results["option"] == {}`、`results["cemesr_ref"] == 0.75`、`plot_dos` の線が −1.0 にあること。akaikkr_cnd の code で `--parameters '{"option": {"cemesr_ref": 0.75}}'` を付けて `results["option"] == {"cemesr_ref": 0.75}`、DOS の mesh が [−1.5, 0.5]。

## 5. 実装順

1. C（parser）。既存ノードには `results["option"]` が無いので、§1・§2.3 は無いときの分岐を持つ。
2. A（plot）。
3. B（presets / inputs / chain / cli / docs）。

A と C は既存ノードに対する読み取りだけなので互換性の問題は無い。B は新規提出の dos の ewidth を変えるので、`example/figures/` の再生成が要る（`docs/examples.md` の表も更新）。
