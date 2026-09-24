# 環境構築

mygardenx2（Ubuntu、sudo 不可、SLURM 単一ノード 24 コア）で 2026-09-24 に行った手順です。すべてユーザー権限で完結します。

## 1. Python 環境

conda 環境 `akaikkr`（Python 3.11）に次を入れます。

```bash
conda activate akaikkr
pip install -e AkaiKKRPythonUtil/library/PyAkaiKKR[ase]
pip install -e AkaiKKRPythonUtil/library/AkaiKKRTestScript
pip install aiida-core            # 2.9.2
pip install -e aiida-akaikkr
verdi plugin list aiida.calculations | grep akaikkr   # 8 個（basic, go, fsm, dos, jij, tc, spc, cnd）
```

`aiida-akaikkr` は `pyproject.toml`（setuptools）でインストールします。以前の `setup.py` + `setup.json` は `reentry_register` を使っていて現行 setuptools では失敗するため削除しました。entry point を変えたら `pip install -e .` をやり直し、daemon を再起動してください。

## 2. RabbitMQ（sudo 不要）

apt が使えないので conda-forge のものを同じ環境に入れ、systemd のユーザーサービスで常駐させます。

```bash
mamba install -n akaikkr -c conda-forge rabbitmq-server   # 4.3.4 + Erlang
```

設定ファイルは `$CONDA_PREFIX/etc/rabbitmq/` です。

`rabbitmq.conf`
```
consumer_timeout = 604800000        # 7 日。AiiDA の長時間プロセス対策
listeners.tcp.local = 127.0.0.1:5672
```

`rabbitmq-env.conf`
```
NODENAME=rabbit@localhost
NODE_IP_ADDRESS=127.0.0.1
```

`~/.config/systemd/user/rabbitmq-akaikkr.service`
```ini
[Unit]
Description=RabbitMQ (conda env akaikkr) for AiiDA
After=network.target

[Service]
Type=simple
Environment=PATH=/home/kino/miniforge3/envs/akaikkr/bin:/usr/bin:/bin
Environment=HOME=%h
ExecStart=/home/kino/miniforge3/envs/akaikkr/bin/rabbitmq-server
ExecStop=/home/kino/miniforge3/envs/akaikkr/bin/rabbitmqctl stop
Restart=on-failure
RestartSec=10
TimeoutStopSec=60

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now rabbitmq-akaikkr
loginctl enable-linger $USER        # ログアウト・再起動後も起動
rabbitmqctl eval 'application:get_env(rabbit, consumer_timeout).'   # {ok,604800000}
```

`rabbitmqctl` には環境の `erl` が必要なので、`conda activate akaikkr` した shell で実行します。

AiiDA は RabbitMQ 3.8.15 以降に警告を出しますが、`consumer_timeout` を延ばしてあるので `verdi config set warnings.rabbitmq_version false` で黙らせています。RabbitMQ 4.3 以降ではこの timeout は quorum queue にしか効かず、AiiDA の classic queue には元々影響しません。

## 3. プロファイル

```bash
verdi presto --profile-name akaikkr     # SQLite ストレージ、computer localhost
verdi profile configure-rabbitmq -n akaikkr --broker-host 127.0.0.1 --broker-port 5672 \
      --broker-username guest --broker-password guest --broker-virtual-host ''
verdi daemon start
verdi status
```

## 4. SLURM computer

```bash
verdi computer setup -n --label mygardenx2-slurm --hostname mygardenx2 \
  --transport core.local --scheduler core.slurm \
  --work-dir /home/kino/aiida_work/{username} --mpirun-command "" \
  --mpiprocs-per-machine 24 --shebang '#!/bin/bash' \
  --prepend-text 'export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1}' --append-text ''
verdi computer configure core.local mygardenx2-slurm -n --safe-interval 5
verdi computer test mygardenx2-slurm
```

- specx は OpenMP 並列なので mpirun は空です。ジョブ側で `num_cores_per_mpiproc` を与えると `--cpus-per-task` になり、prepend text で `OMP_NUM_THREADS` に写ります。
- 既定メモリは設定していません。必要なら `metadata.options.max_memory_kb` で指定します。
- このノードには slurmdbd が無いため、各ジョブに「could not parse scheduler output: detailed_job_info」という警告が付きますが無害です。

## 5. specx の code

3 種類のビルドを別々の code にしてあります。

| label | 実行ファイル |
|---|---|
| specx-akaikkr | AkaiKKRprogram.2022.0721.ifort/akaikkr/specx |
| specx-cnd | AkaiKKRprogram.2022.0721.ifort/akaikkr_cnd/specx |
| specx-cpa2021v01 | AkaiKKRprogram.2022.0721.ifort/akaikkr_cpa2021v01/specx |

```bash
verdi code create core.code.installed -n --label specx-akaikkr --computer mygardenx2-slurm \
  --filepath-executable .../akaikkr/specx --default-calc-job-plugin akaikkr.go \
  --prepend-text 'source /opt/intel/oneapi/setvars.sh --force > /dev/null 2>&1
ulimit -s unlimited
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1}' --append-text ''
```

ifort ランタイム（libifcoremt など）はログインシェルの `LD_LIBRARY_PATH` でしか解決されないので、`setvars.sh` を prepend text で必ず読みます。`OMP_NUM_THREADS` の行が computer と code の両方にあるため sbatch スクリプトに 2 回出ますが問題ありません。

## 6. 動作確認

```bash
verdi process list -a -p 1
verdi calcjob gotocomputer <pk>       # 作業ディレクトリへ
verdi process report <pk>             # パーサーの警告
```
