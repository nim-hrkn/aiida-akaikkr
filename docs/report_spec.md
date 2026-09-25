# HTML レポート（仕様、2026-09-26。実装済み: pyakaikkr.report / kkr-report、aiida_akaikkr.report / `akaikkr-aiida report` / MCP `kkr_report`）

対象: aiida-akaikkr の MCP / CLI から、1 つの計算（go とその後続 dos / spc / jij / cnd、または GAES）の **図と要約を 1 枚の HTML** にして返す。図の描画と HTML の組み立ては pyakaikkr に置き（ファイルからでも AiiDA のノードからでも同じ関数で作れるように）、aiida-akaikkr はノードから材料を集めて渡すだけにする。`pyakaikkr.plot`（配列を受ける「plot 関数 A」）と同じ分担。

## 0. 図の形式: PNG に加えて SVG を出す

- 図は **SVG と PNG の両方**を出す。HTML に埋め込むのは SVG（拡大しても崩れない、テキストが検索できる、ファイル 1 枚に inline で入る）。PNG はこれまでどおり単体の画像として残す（既存の `plot` の出力と互換）。
- 例外は A(w,k) の色メッシュ。nk × ne 個の矩形（例 200 × 400 = 8 万個）を SVG にすると数 MB になり、ブラウザが重くなる。メッシュだけを **rasterize** して SVG に埋める（matplotlib の `QuadMesh.set_rasterized(True)` + `savefig(format="svg")` で、メッシュ部分が PNG として SVG の中に入り、軸・文字・線は vector のまま）。`pyakaikkr.plot.plot_awk` が返す QuadMesh に対して `save_figure` が自動で行う。
- 線の図（DOS、PDOS、Jij、GAES）は vector のまま。DOS 201 点 × 数本なら 1 図 50〜100 KB。
- HTML は **自己完結の 1 ファイル**（SVG は inline、PNG を選んだときは data URI）。MCP はパスを返すだけで済み、コピーしても図が欠けない。
- 既定の埋め込みは SVG。`embed="png"` で PNG の data URI に切り替えられる（SVG が重い環境用）。

## 0.1 言語

HTML は英語（`lang="en"`、既定）と日本語（`lang="ja"`）を選べる。文言は `pyakaikkr.report.T` の辞書（両言語で同じキー）。`<html lang>` も付ける。CLI / MCP の `lang` 引数。

## 1. pyakaikkr 側

### 1.1 `pyakaikkr.plot` への追加

```python
def figure_to_svg(fig, rasterize_meshes=True, dpi=150) -> str      # "<svg ...>...</svg>"（XML 宣言と DOCTYPE を除く）
def figure_to_png(fig, dpi=150) -> bytes
def save_figure(fig, path_noext, formats=("png", "svg"), rasterize_meshes=True, dpi=150) -> dict   # {"png": path, "svg": path}
def data_uri(png_bytes) -> str                                          # "data:image/png;base64,..."
```

- `rasterize_meshes=True` は Figure 内の `QuadMesh` / `AxesImage` に `set_rasterized(True)` を掛けてから保存する（`plot_awk` の戻り値をわざわざ扱わなくてよい）。
- 既存の描画関数（`plot_dos`, `plot_pdos`, `plot_awk`, `plot_jij`, `plot_gaes_dos`）は変えない。

### 1.2 新モジュール `pyakaikkr.report`

データ（材料）と描画と HTML 化を分ける。

```python
@dataclass
class Component:            # 1 type の 1 成分
    type: str               # AkaiKKR の type 名（例 "Pt0.5Rh0.5_1d_1"）
    element: str            # "Rh"
    z: float
    conc: float             # 0-1
    spin_moment: float | None
    orbital_moment: float | None
    charge: float | None    # type_charge（成分ごとに出るものは成分ごと、type ごとなら type の値）
    core_levels: dict | None    # {"4d": [E - E_F, star], ...}（GAES で使う。省略可）

@dataclass
class ReportData:
    title: str                          # 表題（label や key）
    formula: str                        # 約分した式（"Fe(Rh0.5Pt0.5)"、CPA は括弧内に分率）
    formula_full: str                   # 胞全体の式（"Fe1 Rh0.5 Pt0.5"）
    structure_source: dict              # {"cif": "FeRh0.5Pt0.5.cif", "poscar": None, "preset": "FeRh05Pt05", "comp": None}
    spacegroup: dict | None             # {"number": 221, "symbol": "Pm-3m", "symprec": 1e-3, "n_sites": 2}
    lattice: dict                       # {"brvtyp": "sc", "a_bohr": 5.7, "a_angstrom": ..., "c/a": 1.0, "b/a": 1.0, "alpha": 90, ...}
    natm: int
    calc: dict                          # {"code": "specx-akaikkr@mygardenx2-slurm", "build": "akaikkr", "magtyp": "mag", "sdftyp": "pbe",
                                        #  "reltyp": "sra", "ewidth": 1.2, "edelt": 1e-4, "bzqlty": 10, "maxitr": 200, "pmix": 0.02}
    scf: dict                           # {"converged": True, "n_iter": 45, "rms_last": -6.1, "fermi_level": 0.635,
                                        #  "total_energy_Ry": -3309.96, "total_moment": 0.0}
    components: list[Component]
    tc: float | None                    # jij の Tc（K）
    cnd: dict | None                    # {"resistivity": 1.2e-6, "conductivity": [...], "unit": "..."}
    gaes: dict | None                   # {"status", "ewidth", "history": [...]}（GAES のとき）
    provenance: list[dict]              # [{"step": "go", "pk": 3743, "workdir": "...", "computer": "..."}, ...] または {"step", "directory"}
    figures: list[Figure] = field(default_factory=list)

@dataclass
class Figure:
    name: str                           # "dos", "pdos", "awk_up", "awk_dn", "jij_<t1>-<t2>", "gaes"
    caption: str
    svg: str | None
    png: bytes | None
```

生成:

```python
# ファイルから（pyakaikkr 単体、testrun / kkr-gaes の出力）
def report_from_directory(directory, outfiles=None, structure_files=None, title=None) -> ReportData
    # outfiles 既定 {"go": "out_go.log", "dos": "out_dos.log", "spc": "out_spc.log", "j": "out_j.log", "cnd": "out_cnd.log"}
    # 無いファイルは飛ばす。structure_files={"cif": path, "poscar": path} は名前と対称性の出典に使う（無ければ go の構造から）
    # 数値は AkaikkrJob.get_*（total_energy, total_moment, local_moment, type_charge, fermi_level, ewidth, edelt,
    # convergence, type_of_site, curie_temperature, resistivity, conductivity, core_levels_by_component）
    # 構造は make_pymatgenstructure → formula / symmetry

def symmetry_of(structure, symprec=1e-3) -> dict            # pymatgen SpacegroupAnalyzer（部分占有もそのまま扱える）
def formula_of(structure) -> tuple[str, str]                 # (約分式, 胞全体の式)。CPA は "Fe(Rh0.5Pt0.5)" 形式

# 図（配列を受ける plot 関数 A を呼ぶ。data に追加する）
def add_dos_figure(data, energy, dos, ewidth_go, pdos=None, component_names=None, style=None)
def add_awk_figures(data, awk_up, awk_dn, kdist, energy, kcrt, klabel, style=None)   # AwkReader の配列
def add_jij_figures(data, jij_table, component_names, tc=None, style=None)          # DataFrame / dict の列
def add_gaes_figure(data, history_entry, energy, dos, parameters, style=None)

# HTML
def render_html(data: ReportData, embed="svg", css=None) -> str
def write_report(data, path, embed="svg") -> str            # path を返す
```

HTML の構成（依存ライブラリ無し、`html.escape` と f-string、CSS は数十行を内蔵）:

1. 表題、式（約分 / 胞全体）、構造の出典（CIF / POSCAR / preset / comp）、空間群（番号、記号、symprec）、格子（brvtyp、a、c/a、角）、原子数。
2. 計算条件（code / build、magtyp、sdftyp、reltyp、ewidth、edelt、bzqlty、maxitr、pmix）。
3. SCF の結果（収束、反復数、E_F、全エネルギー、全モーメント）。
4. 成分の表（type、元素、濃度、spin / orbital モーメント、電荷）。
5. 図: DOS（go の ewidth 線付き）、PDOS（成分ごと）、A(w,k)（up / down）、Jij（type 対ごと、Tc）、GAES（あれば）。各図に 1 行の説明（caption）。
6. 輸送（cnd があれば抵抗率・伝導度）。
7. provenance（pk と作業ディレクトリ、またはディレクトリ名）。

### 1.3 `kkr-report` CLI（pyakaikkr）

```
kkr-report <directory> [--out report.html] [--embed svg|png] [--cif x.cif] [--poscar POSCAR] [--title T]
```

## 2. aiida-akaikkr 側

### 2.1 `aiida_akaikkr/report.py`

```python
def collect_report_data(pk, dos_pk=None, spc_pk=None, jij_pk=None, cnd_pk=None, gaes_pk=None, symprec=1e-3) -> ReportData
```

- `pk` は次のどれでもよい。
  - `AkaikkrChainWorkChain`: 子の go と後続（dos / spc / jij / cnd / tc / fsm）をリンクラベルから集める。
  - `AkaikkrGaesWorkChain`: 最後の go / dos と history（GAES の図と表）。
  - go の CalcJob: `outputs.potential` を入力にした子孫（dos / spc / jij / cnd）を QueryBuilder で集める。明示の `*_pk` があればそれを優先。
- 構造: go の `outputs.structure`（StructureData → pymatgen）から式と対称性。出典は provenance を遡る: `make_common_param` の `cif`（SinglefileData の `filename`）と `func_name`（preset 名）、`single_site_common_param` の `comp` 文字列。POSCAR から作った CIF は、`make_common_param` に `source` を残す（未実装、追加する）。
- 数値: go の `results`（total_energy、total_moment、local_moment、type_charge、fermi_level、ewidth、edelt、convervence、type_of_site）、jij の `Tc`、cnd の `resistivity` / `conductivity`、GAES の `ewidth` / `status` / `history`。
- 図: `pyakaikkr.report.add_*` に ArrayData / Dict の配列を渡す（`plot.py` と同じ経路、配色 `STYLE`）。
- provenance: 各 CalcJob の pk、computer、`remote_workdir`、SLURM job id。

```python
def write_report(pk, outdir=None, prefix=None, embed="svg", **pks) -> dict
    # {"html": path, "figures": [png paths], "summary": {"formula", "spacegroup", "total_energy", "total_moment", "tc", "resistivity"}}
```

### 2.2 CLI / MCP

```
akaikkr-aiida report --pk <pk> [--dos-pk --spc-pk --jij-pk --cnd-pk --gaes-pk] [--outdir DIR] [--prefix P] [--embed svg|png]
```

- `cli/spec.py` の `SUBCOMMANDS` に `report`（read）を追加 → MCP に `kkr_report(pk, dos_pk=None, spc_pk=None, jij_pk=None, cnd_pk=None, gaes_pk=None, outdir=None, prefix=None, embed="svg")` が自動で生える。戻り値は JSON（html のパス、図のパス、要約）。HTML 本文は返さない（大きい）。本文が要るホスト向けに `inline=True` で `"html_text"` を付ける option を残す。
- 出力先の既定は `~/aiida_work/figures/<pk>/`（`plot` と同じ）。

### 2.3 pyakaikkr との分担

| 処理 | 場所 |
|---|---|
| 配列 → 図（Axes） | `pyakaikkr.plot`（既存） |
| Figure → SVG / PNG | `pyakaikkr.plot.figure_to_svg / figure_to_png / save_figure`（新規） |
| 材料の器と HTML 化 | `pyakaikkr.report`（新規） |
| ファイル → 材料 | `pyakaikkr.report.report_from_directory`（新規、`kkr-report`） |
| ノード → 材料 | `aiida_akaikkr.report.collect_report_data`（新規、`akaikkr-aiida report`、MCP `kkr_report`） |

## 3. テスト

- pyakaikkr `tests/plot/test_report.py`（specx 不要）: 合成配列から `ReportData` を作り、`render_html` が式・空間群・表・`<svg` を含むこと、`figure_to_svg` がメッシュを rasterize すること（SVG 内に `<image` が 1 つ）、`embed="png"` で data URI になること。
- pyakaikkr `tests/gaes/test_gaes_run.py` 相当の実走テスト: testrun の `Cu/` 出力から `report_from_directory` → HTML。
- aiida-akaikkr `tests/test_report.py`: `report` が spec と MCP に出ること、`collect_report_data` が偽ノード（dict で代用）から `ReportData` を組み立てること。実走は Cu の chain（go + dos + spc + jij）で 1 回。

## 4. 実装の順序

1. `pyakaikkr.plot` に `figure_to_svg` / `figure_to_png` / `save_figure`（rasterize）を追加、`plot.py`（aiida）の `savefig` を `save_figure` に置き換えて PNG + SVG を出す。
2. `pyakaikkr.report`（dataclass、`symmetry_of` / `formula_of`、`add_*`、`render_html`、`report_from_directory`、`kkr-report`）。
3. `aiida_akaikkr.report`（`collect_report_data`、`write_report`）、`cli/spec.py` に `report`、MCP、docs。
4. テストと、Cu / FeRh0.5Pt0.5 / GAES の見本 HTML を `docs/data/` に置く。

## 5. 見本（2026-09-26）

- `docs/data/report_FeRh05Pt05_aiida_{en,ja}.html`: go pk 2682（FeRh0.5Pt0.5、mygardenx2）と potential のリンクで見つかる dos / spc / jij / cnd。出典は CIF `FeRh0.5Pt0.5.cif` と preset `FeRh05Pt05`（`split_param` ← `make_common_param` を遡る）。Tc 217.2 K、抵抗率 12.46。
- `docs/data/report_Cu_gaes_aiida_ja.html`: GAES WorkChain pk 3730（Cu 単一サイト CPA、mygardenx1）。GAES の節と図付き。
- pyakaikkr 単体（testrun の出力から）: AkaiKKRPythonUtil/docs/data/report_FeRh05Pt05_{en,ja}.html、report_SmCo5_oc_ja.html。

テスト: pyakaikkr `tests/plot/test_report.py`（7 件）、aiida-akaikkr `tests/test_report.py`（3 件。既存の chain があればレポートを作る）。
