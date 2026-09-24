# 既知の問題と参照値との差

2026-09-24、mygardenx2（ifort ビルド、OpenMP 8 スレッド）での結果です。参照は AkaiKKRPythonUtil の `tests/akaikkr/reference/ifort.json` と `tests/akaikkr_cnd/reference/ifort.json`（128 スレッド EPYC 7702 で作成）。

## 1. 結果の要約

| セット | CalcJob | exit 0 | te の一致 |
|---|---|---|---|
| akaikkr（13 物質） | 65 | 65 | 62 件が rdiff ≤ 1e-9 |
| akaikkr_cnd（12 物質、cnd 3 件） | 63 | 63 | 抵抗率・伝導度は最終桁まで一致 |

一致しない件:

| 計算 | te rdiff | 備考 |
|---|---|---|
| Co2MnSi_dos, Co2MnSi_spc31（akaikkr） | 1.1e-5, 1.3e-5 | 原因未特定。go / tc / j3.0 / fsm は 1e-11 で一致。inputcard はテストのものと aux/fcc の表記以外同じ |
| Fe_lmd_dos, Fe_lmd_spc31（akaikkr） | 3.9e-7, 3.8e-8 | lmd は cnd テストスイートでも参照と最終桁が合わない |
| Fe_lmd_dos, Fe_lmd_spc31（cnd） | 3.3e-6, 1.8e-6 | 同上 |
| Co2MnSi_spc31（cnd） | 8.3e-7 | |
| Ni_tc など Tc | 0.005 K 以内 | 最終桁 |

FeB1.95 は cnd の参照に無いので比較していません。

## 2. パーサーの制限

- structure 出力: CPA は Kind の weights で保持しますが、空球（Og）は除かれます。lmd は出力しません。
- pdos: 型ごとに l の数が違うと NaN で埋まります。`nl_per_type` を見てください。
- spc: `structure_data` を渡す必要があります。CIF から直接 StructureData を作ると型名や基本胞が go と食い違うことがあるので、go の structure 出力を使ってください。

## 3. 環境由来の警告

- `could not parse scheduler output: return value of detailed_job_info is non-zero`: このノードに slurmdbd/sacct が無いため。無害。
- RabbitMQ 4.3.4 のバージョン警告: `consumer_timeout` を 7 日に設定済み。`warnings.rabbitmq_version` を false にしてある。
- sbatch スクリプトに `export OMP_NUM_THREADS` が 2 回出る: computer と code の prepend text の重複。無害。

## 4. 未対応

- gofmg（fmg でモーメントを反転してから go）。
- `record` の自動判定（`pyakaikkr.ase.AkaiKKR` にはある）。
- forces / stress。AkaiKKR は出しません。
- `example/run_go.ipynb` は古く未検証。
