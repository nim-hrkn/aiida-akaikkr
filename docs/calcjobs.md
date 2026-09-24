# CalcJob とパーサー

ソースは `aiida_akaikkr/calculations/akaikkr_calcjob.py` と `aiida_akaikkr/parsers/akaikkr_parser.py` です。

## 1. CalcJob 一覧

| entry point | クラス | `go` の値 | 追加入力 | 追加出力 | pot.dat 回収 |
|---|---|---|---|---|---|
| akaikkr.basic | specx_basic | 入力 `go` | | | 入力 `retrieve_potential` |
| akaikkr.go | specx_go | `go` | | `potential` | する |
| akaikkr.fsm | specx_fsm | `fsm` | `fspin` (Float) | `potential` | する |
| akaikkr.dos | specx_dos | `dos` | | `dos`, `pdos` (ArrayData) | しない |
| akaikkr.jij | specx_jij | `j3.0` | | `Jij` (Dict), `Tc` (Float) | しない |
| akaikkr.tc | specx_tc | `tc` | | `Tc` (Float) | しない |
| akaikkr.spc | specx_spc | `spc31` | `structure_data` (StructureData), `nk` (Int, 150) | `Awk_up`, `Awk_dn` (SinglefileData), `klabel` (Dict) | しない |
| akaikkr.cnd | specx_cnd | `" cnd"` | | `resistivity` (Float), `conductivity` (List) | しない |

`cnd` の `go` は先頭に空白が必要です（specx の仕様）。akaikkr_cnd ビルドでのみ使えます。

## 2. 共通の入力

| ポート | 型 | 意味 |
|---|---|---|
| code | InstalledCode | specx |
| structure | Dict | AkaiKKR 形式の構造パラメータ。`brvtyp, a, c/a, b/a, alpha, beta, gamma, r1..r3, ntyp, type, ncmp, rmt, field, mxl, anclr, conc, natm, atmicx` と、あれば `displc` |
| parameters | Dict | それ以外の入力（`edelt, ewidth, reltyp, sdftyp, record, outtyp, bzqlty, maxitr, pmix` など）。`AkaikkrJob("dummy").default` を土台にするのが簡単 |
| magtype | Str | `nmag` / `mag` / `lmd`。`parameters` の `magtyp` より優先 |
| displc | Bool | True なら `structure` に `displc` が無いとき `make_displc_list(anclr)` で零ベクトルを付ける。akaikkr_cnd は displc 行が無いと "illegal input" で止まる |
| potential | Str / SinglefileData / RemoteData | 既定は `Str("")`（コピーしない）。SinglefileData は `pot.dat` としてローカルコピー、RemoteData はその remote folder の `pot.dat` をリモートコピー |
| retrieve_potential | Bool | 計算後の `pot.dat` を回収するか |
| metadata.options.input_filename / output_filename | str | 既定 `go.in` / `go.out` |

inputcard は `AkaikkrJob.make_inputcard(parameters ∪ structure ∪ {magtyp, go})` で書きます。`structure` の内容が `parameters` を上書きします。

`record` の扱いはプラグインでは何もしません。`init` は `potential` 無しで、`2nd` は go の `potential` を渡して使ってください。

## 3. パーサーの出力

`results`（Dict）は `get_basic_properties` の結果で、`AkaikkrJob.get_*` をまとめたものです。主なキー: `total_energy, total_moment, local_moment, type_charge, fermi_level, convervence`（綴りは元のまま）, `rms_error, err_history, te_history, moment_history, type_of_site, prim_vec, atom_coords, atom_names, core_level` など。

`structure`（StructureData）は `AkaikkrJob.make_pymatgenstructure` の pymatgen 構造から `StructureData(pymatgen=...)` で作ります。

- CPA の部分占有は Kind の `symbols` / `weights` として残ります（例 `(('Fe','Co'), (0.5, 0.5))`）。pymatgen の `AseAtomsAdaptor.get_atoms` は部分占有を受け付けない（`ValueError: ASE Atoms only supports ordered structures`）ので、ASE 経由にはしていません。
- 空球サイト（pyakaikkr は `Og`、pymatgen では空の species）は AiiDA に元素として登録できない（Z ≤ 116）ため、警告を出して除いてから StructureData にします（GaAs の Vc など）。
- `magtype = lmd` では structure を出力しません。
- 失敗しても exit code にはせず、警告だけで structure を省きます。

`dos`（ArrayData）: `energy` (nE,)、`dos` (nspin, nE)。エネルギーは E_F 基準の Ry。

`pdos`（ArrayData）: `energy` (nE,)、`pdos` (nspin, ncomp, nE, nl_max)、`nl_per_type` (ncomp,)。第 2 軸は type ではなく CPA 成分を平坦に並べたものです（NiFe なら Fe, Ni の 2 つ）。型ごとに `mxl` が違うと l の数が違うので、足りない分は NaN で埋めてあります。

`Jij`（Dict）: `get_jij_as_dataframe` の各列をリストにしたもの。`Tc`: `get_curie_temperature`。

`Awk_up` / `Awk_dn`: `pot.dat_up.spc` / `pot.dat_dn.spc` をそのまま。`nmag` では up だけ。`klabel`: `HighSymKPath` が書いた `klabel.json`（`{"kpath": [[{"\\Gamma": [0,0,0]}, {"X": [...]}, ...]]}`）。

`resistivity` / `conductivity`: `get_resistivity` / `get_conductivity`（スピン別のリスト）。

## 4. exit code

| code | 意味 |
|---|---|
| 200 | retrieved folder が読めない |
| 302 / 310 / 311 / 312 | stdout が無い / 読めない / 解析できない / 途中で切れている |
| 320 | stdin（inputcard）が回収されていない |
| 321 | `pot.dat` が回収されていない（specx が inputcard を拒否したときに出ることが多い。`go.out` の先頭数行を見る） |
| 322 / 323 | `.spc` / `klabel.json` が無い |
| 324 / 325 / 326 / 327 / 328 | DOS / PDOS / Jij / Tc / 抵抗率の解析失敗 |
| 390 | パーサーの予期しない例外 |

## 5. spc31 の k 経路

`specx_spc` は `structure_data` の pymatgen 構造から `pyakaikkr.HighsymmetryKpath.HighSymKPath` で経路を作り、`kpath_raw` として inputcard に書きます（`nk` 点、fmt 3、`first_connected_kpath=False`）。`structure_data` には go の `structure` 出力を渡すのが通常です。lmd の go は structure を出さないので、同じ格子の非 lmd 計算の structure を渡してください。

## 6. 2022 年版からの修正点（2026-09-24）

- `KKRValueAquisitionError` が dos 分岐でしか import されておらず、go の解析失敗が NameError になっていた。
- `inputs.magtype` を `.value` なしで文字列比較していたため nmag の分岐が効かなかった。
- spc の回収チェックが `_up.spc` を 3 回見て `_dn.spc` を見ていなかった。
- tc 単独実行で `job` が未定義になる分岐があった。
- `specx_dos` の `go` 既定値が `Str` でなかった。`specx_fsm` が `magtyp` を書いていなかった。
- `displc` 入力が `prepare_for_submission` で使われていなかった。
- `DataFactory` の名前を `core.*` に変更。
- structure 出力を ASE 経由から pymatgen 直接に変更（CPA 対応）。PDOS の NaN 埋め、空球の除去、`specx_cnd` を追加。
