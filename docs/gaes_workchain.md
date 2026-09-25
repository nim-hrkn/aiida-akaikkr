# GAES WorkChain（ewidth の自動決定）

対象: aiida-akaikkr `AkaikkrGaesWorkChain`（entry point `akaikkr.gaes`、2026-09-25 実装）。アルゴリズムは AkaiKKRPythonUtil/docs/ewidth_tuning_scheme.md（GAES = Gap-Anchored Ewidth Search）、判定関数は `pyakaikkr.gaes.decide`。

## 何をするか

go の `ewidth`（積分路の下端 E_F − ewidth）が、valence 帯と semicore / core の間のバンドギャップ（連続した energy mesh で DOS < threshold）に入るまで、次を繰り返す。

1. `go`（新しいポテンシャルから、`record=init`、edelt 1e-4、maxitr 500、pmix 0.005）
2. `dos`（go のポテンシャルから、edelt 1e-4 固定、窓 `ewidth_dos` は E_F − ewidth − eth − ediff まで届くよう自動で広げる、上限 4.5 Ry）
3. 判定（Method 2 既定: 粗い threshold 2e-2 で幅 0.3 Ry 以上の区間、その中の細かい threshold 1e-3 の部分区間に、valence 帯の底から 0.2 Ry の余裕を取って ewidth を置く）。ギャップ区間の判定は dos の窓全体で行い、`min_ewidth` / `max_ewidth` には依らない。範囲 [min_ewidth, max_ewidth]（既定 1.0〜2.0 Ry）は ewidth を選ぶときだけ使い、範囲外の候補は端へ寄せる。`old` なら終了、`new` なら次の ewidth、`fail` なら終了。
4. 低 DOS 区間が dos の窓の下端に接するときは、go を回さず窓を 1.5 倍にして dos だけ取り直す（最大 3 回）。

SCF が収束していなくても判定する（ewidth がギャップに無いこと自体が収束を妨げるため）。この版には edelt / pmix を落として SCF を追い込む STEP2 は無い。

## 入力

| ポート | 型 | 内容 |
|---|---|---|
| code | Code | specx |
| common | Dict | 構造 + パラメタ（`inputs.make_common_param` / `preset_common_param` / `single_site_common_param`） |
| gaes | Dict | `GAES_DEFAULTS` の上書き: ewidth_init 1.2, method 2, dosth 2e-2, dosth2 1e-3, dosth2_relax 2, eth 0.30, ediff 0.20, margin 0.01, min_ewidth 1.0, max_ewidth 2.0, max_ew 10, ewidth_dos 3.0, ewidth_dos_auto True, ewidth_dos_max 4.5, ref 0.75, edelt_init 1e-4, edelt_dos 1e-4, maxitr 500, pmix 0.005 |
| overrides | Dict | 全ジョブに掛ける AkaiKKR パラメタ（bzqlty、sdftyp、option など） |
| displc, ncores, wallclock, label | | chain と同じ |

`ref` は使うビルドの dos の窓に合わせる（akaikkr 0.75、cpa2021v01 / cnd 0.5）。CLI はコード名に `cnd` / `cpa2021` を含むとき 0.5 にする。

## 出力

| ポート | 型 | 内容 |
|---|---|---|
| ewidth | Float | 採用した ewidth（`finished`）、または最後に試した値 |
| status | Str | `finished` / `ewidth_fail` / `ewidth_exhausted` / `not_converged` |
| history | List | 判定ごとの dict: iew, ewidth, converged, flag（old / new / fail / window_limited）, ewidth_dos, window, coarse_regions, fine_regions, candidates, gap_used, relaxed, dosth2_used, go_pk, dos_pk |
| parameters | Dict | 実際に使った GAES パラメタ |
| go.*, dos.* | | 最後の go / dos の出力（results, potential, structure, dos, pdos） |

終了コード: 400 go 失敗、401 dos 失敗、420 `ewidth_fail`、421 `ewidth_exhausted`、422 候補が尽きた。`finished` 以外でも history と最後の go / dos は出力される。

## 使い方

```
akaikkr-aiida submit-gaes --comp AlSiRhBi --polytyp fcc --code specx-akaikkr@mygardenx2-slurm --label AlSiRhBi
akaikkr-aiida submit-gaes --preset Cu --code specx-akaikkr@mygardenx2-slurm --ewidth-init 1.6
akaikkr-aiida submit-gaes --structure-pk 2866 --code ... --method 1 --dosth 2e-2
akaikkr-aiida results --pk <wc pk>              # ewidth, status, history, 各 go の要約
akaikkr-aiida plot --gaes-pk <wc pk>             # 反復ごとの DOS（緑: 粗い区間、青: 細かい区間、斜線: [min_ewidth, max_ewidth] の帯、赤: その go の -ewidth）と重ね描き
```

MCP: `kkr_submit_gaes(comp="AlSiRhBi", polytyp="fcc", code="...")`, `kkr_results(pk)`, `kkr_plot(gaes_pk=...)`。

`--comp` は単一サイト CPA（1 type、1 原子、`a=1000000` = 実験原子体積の濃度平均、bzqlty 10、pbe、sra、mag）で、`Rh0.5Pt0.5` のような任意の比も書ける。`single_site_common_param` が calcfunction として Dict を作るので provenance に残る。

## 注意

- AkaiKKR 2022.0721 の akaikkr ビルドは Hf を含む go が `reconf` で止まる（ewidth_tuning_scheme.md §13.1）。Hf 系は `specx-cpa2021v01` を使う（dos の窓は ref 0.5）。
- 判定用 dos の窓は 4.5 Ry まで。それより広い窓の DOS は −3 Ry 以深で信用できない。
- 図の線は go の ewidth（`aiida_akaikkr.plot.contour_bottom`）。dos の ewidth ではない。
