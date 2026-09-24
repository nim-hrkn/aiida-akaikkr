# example/ のスクリプト

すべて `conda activate akaikkr` した shell で、`aiida-akaikkr` ディレクトリから実行します。構造パラメータの生成で specx を geometry モードで一度ローカル実行しますが、一時ディレクトリ内で行うのでカレントにファイルは残りません。

## 1. run_cu_go_dos_spc.py（単発）

fcc Cu の go → dos → spc31 を daemon 経由で流します。

```bash
python example/run_cu_go_dos_spc.py --code specx-akaikkr@mygardenx2-slurm --ncores 8
```

- CIF は `SinglefileData` として保存され、`make_structure_param`（calcfunction）の入力になります。specx は `InstalledCode` ノードで渡し、内部で `code.filepath_executable` を使います。パス文字列は provenance に残りません。
- 構造パラメータは `akaikkr_testscript.get_kkr_struc_from_cif` で作り、type 名を `Cu` に直しています。
- dos と spc は go の `potential` を `SinglefileData` で受け取り、spc は go の `structure` を `structure_data` に使います。

## 2. run_examples.py（テストセット一括）

AkaiKKRPythonUtil の `tests/akaikkr`（および `tests/akaikkr_cnd`）と同じ物質・同じパラメータで実行し、`tests/*/reference/ifort.json` と比べられるようにしたものです。

```bash
# akaikkr セット（13 物質、65 CalcJob）
python example/run_examples.py --code specx-akaikkr@mygardenx2-slurm --ncores 8
# akaikkr_cnd セット（displc 付き、NiFe / FeRh0.5Pt0.5 / AlMnFeCo に cnd を追加、63 CalcJob）
python example/run_examples.py --code specx-cnd@mygardenx2-slurm --displc --result run_cnd_result.json \
    --only Cu,Fe,Co,Ni,NiFe,FeRh05Pt05,AlMnFeCo_bcc,Fe_lmd,FeB195,GaAs,Co2MnSi,SmCo5_oc
```

| オプション | 意味 |
|---|---|
| `--code` | specx の code label |
| `--only A,B` | 物質を絞る |
| `--ncores` | `num_cores_per_mpiproc`（OpenMP スレッド数、既定 8） |
| `--wallclock` | 秒（既定 7200） |
| `--displc` | displc を付ける。akaikkr_cnd では必須。`CND_MATERIALS` に cnd ステップを追加 |
| `--result` | 結果 JSON のファイル名 |
| `--structure-dir` | CIF の場所（既定 `example/structure`） |

動作:

1. 各物質の共通パラメータを `akaikkr_testscript.testrun_class._<名前>_common_param` で作る（calcfunction `make_common_param`、入力は関数名・`InstalledCode`・CIF の `SinglefileData`・displc）。
2. calcfunction `split_param` が共通 Dict を `structure`（格子・型・原子）と `parameters`（それ以外 + モード別の上書き）と `magtype` に分ける。モード別の上書きはテストスクリプトの `Go*` クラスと同じ。

   | mode | record | その他 |
   |---|---|---|
   | go | init | |
   | dos | 2nd | ewidth 2.0 |
   | tc / j3.0 / spc | 2nd | |
   | fsm | init（`FSM_FROM_GO_POTENTIAL` の物質は 2nd で go の potential から） | fspin、pmix 0.02ch |
   | cnd | 2nd | ewidth 0.01、bzqlty 40 |

3. 全物質の go を投入して待ち、成功したものだけ後続モードを投入する。spc の `structure_data` は go の structure（Fe_lmd は Fe の go から）。
4. 結果を JSON に書く（pk、exit、te、moment、Tc、resis、cnd）。

物質と後続モードは `MATERIALS` 辞書で定義しています。SmCo5_noc は go のみ。gofmg（fmg によるモーメント反転）は未対応です。

## 3. compare_reference.py（参照値との比較）

```bash
python example/compare_reference.py run_examples_result.json \
    ../AkaiKKRPythonUtil/tests/akaikkr/reference/ifort.json
```

te の相対差、moment、Tc、抵抗率・伝導度を並べます。2026-09-24 の結果は [known_issues.md](known_issues.md) にまとめてあります。

## 4. plot_results.py（図）

```bash
python example/plot_results.py --dos <dos pk> --spc <spc pk> --jij <j3.0 pk> --outdir example/figures --prefix Fe
```

`<prefix>_dos.png`（全 DOS、mag は up を +、down を −）、`<prefix>_pdos.png`（CPA 成分ごと、s/p/d/f）、`<prefix>_Awk_up.png`（nmag 以外は `_Awk_dn.png` も）を書きます。横軸は AkaiKKR 出力どおり E_F 基準の Ry です。

DOS / PDOS には E − E_F = −|ewidth| の一点鎖線を入れます。ewidth はポテンシャルを作った go（`inputs.potential.creator`）のもので、SCF のエネルギー積分路の下端（cemesh.f: ebtm = ef − ewidth）です。この線より下の状態は自己無撞着な電荷に入っていないので、価電子帯の底が線より右にあることを確かめてください。go が provenance に無いとき（ポテンシャルをファイルから与えたとき）は dos 自身の ewidth を使います。dos のエネルギー mesh は [E_F − ref·ewidth, E_F + (1 − ref)·ewidth]（cemesr.f、ref は akaikkr ビルドで 0.75、akaikkr_cnd ビルドでは 0.5 が既定。begin_option の `cemesr_ref=` で変更）なので、線が mesh の外に落ちるときは横軸を線まで広げます。go と dos の ewidth の違いは [ewidth.md](ewidth.md) を見てください。`example/figures/` には 12 物質分が入っています。

`--jij` は AkaiKKRPythonUtil の testrun の j30 後処理（`Goj30.postscript` の `jij.csv` と `JijPlotter.make_typepair` / `make_comppair` の `Jij_*.png`）に相当します。

- `<prefix>_jij.csv`: `Jij` 出力 Dict をそのまま表にしたもの（列は `get_jij_as_dataframe` と同じ: site1, site2, comp1, comp2, a, b, c, distance, J_ij, J_ij(meV), dgn, type1, type2）。
- `<prefix>_Jij_<type1>-<type2>.png`: type 対ごとに 1 枚。CPA の type では (comp1, comp2) の成分対ごとに 1 パネル（AlMnFeCo なら 10 パネル、FeRh0.5Pt0.5 の Fe-RhPt なら Fe-Rh と Fe-Pt の 2 パネル）で、成分名は `results["type_of_site"]` の `comp_shortname` から取ります。横軸は AkaiKKR 出力どおり格子定数 a 単位の距離、縦軸は meV、1 枚の中で軸範囲は共通、題に `Tc` を添えます。

`run_examples.py --figdir example/figures` を付けると、走り終わった dos / spc / j3.0 の図と CSV をまとめて書きます。`example/figures/` には Fe, Co, Ni, NiFe, FeRh05Pt05, AlMnFeCo_bcc, Co2MnSi, SmCo5_oc の J_ij が入っています（2026-09-24、akaikkr セットの pk 1836〜2324）。

## 5. provenance graph

```bash
verdi node graph generate <go pk> --ancestor-depth 5 --descendant-depth 3 --process-in --process-out -f pdf -O Cu_provenance
```

Graphviz の `dot` は conda-forge から入れました（`mamba install -n akaikkr -c conda-forge graphviz`）。`example/figures/Cu_provenance.{png,pdf}` と `FeRh05Pt05_provenance.{png,pdf}` があります。祖先深さ 5 で CIF の SinglefileData と InstalledCode から go までが入り、子孫深さ 3 で go の potential / structure を受けた後続ジョブとその出力まで入ります。

## 6. run_go.ipynb

2022 年の notebook です。`tools.aiida_support.wait_for_node_finished` と `get_kkr_struc_from_cif` を Str のパスで呼ぶ古い書き方で、現行環境では未検証です。上のスクリプトを参照してください。
