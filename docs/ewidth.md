# go と dos の ewidth と ref

同じ `ewidth` という入力でも、go と dos とで意味が違います。ソース（AkaiKKRprogram.2022.0721.ifort）とマニュアル（docs/manual/ja/manual1_akaikkr.pdf §6.3、§7.1、manual2 注 11）を突き合わせた記録です。

## go: 収束させる計算。[E_F − ewidth, E_F] を積分する

go は電子状態を自己無撞着に収束させるモードで、電荷密度は Green 関数を複素平面上の積分路に沿って積分して求めます。積分路は `cemesh.f` が作り、下端は

```
ebtm = ef - ewidth        (cemesh.f)
```

です。つまり **[E_F − ewidth, E_F] の範囲の状態だけが価電子として電荷に入ります**。それより下の状態は取りこぼされるので、マニュアルは「ewidth の大きさは価電子帯を完全にカバーするものでなければならない」と書いています。

ewidth を決める手順もマニュアルにあります。価電子帯の幅がわからないときは、大きめの ewidth で荒く go を回し、dos で状態密度を出して価電子帯の底を読み、余裕を加えた ewidth で go をやり直す。

大きすぎる ewidth は、反復ごとの積分点数（`mse0`）が固定なので積分が粗くなります。価電子帯の底に少し余裕を持たせる程度が適切です。

## dos: 収束した電子状態に対して DOS を描く。[E_F − ref·ewidth, E_F + (1 − ref)·ewidth]

dos は `record=2nd` で go のポテンシャルを読み、それを固定したまま実軸に沿った Green 関数を計算して状態密度を書くモードです。収束はしません。メッシュは `drvmsh.f` が `cemesr.f` を呼んで作ります。

```fortran
      data ref/0.75d0/                ! akaikkr ビルド。akaikkr_cnd ビルドは 0.5d0
      ref = optnwrt_ref(ref)          ! begin_option の cemesr_ref= があれば上書き
      de=ewidth/dble(kmx-1)           ! 刻み: 全幅 ewidth を kmx-1 等分
      kef=dble(kmx-1)*ref+1           ! E_F が来るメッシュ番号
      el=ef-de*dble(kef-1)            ! 下端 = ef - ref*ewidth
```

したがって dos の ewidth は **メッシュの全幅** で、範囲は

```
[E_F − ref·ewidth,  E_F + (1 − ref)·ewidth]
```

です。E_F の上にも (1 − ref)·ewidth だけ描かれます。go の ewidth のように「E_F の下 ewidth まで」ではありません。出力の `total DOS` の横軸は `e(k) − estep/2 − ef`（spmain.f）なので、窓の端より半刻み内側から始まります（ewidth 2.0、200 点なら −0.995）。

ref の既定値はビルドで違います。

| ビルド | `cemesr.f` の `data ref` | ewidth 2.0 のときの窓 |
|---|---|---|
| akaikkr（specx-akaikkr） | 0.75 | [−1.5, +0.5] |
| akaikkr_cnd（specx-cnd） | 0.5 | [−1.0, +1.0] |

変えたいときは inputcard の末尾に書きます（`akaikkr_common/source/m_optn.f` が `cemesr_ref=` を読んで `optnwrt_ref` で返す）。

```
begin_option
cemesr_ref= 1.0
end_option
```

pyakaikkr では `make_inputcard` の `option` 辞書に `{"cemesr_ref": 1.0}` を渡します。キーの一覧は AkaiKKRPythonUtil/docs/akaikkr_option_keys.md にあります。

## go と dos で ewidth を変える

go と dos の ewidth は独立です。dos の ewidth と ref はメッシュの範囲を決めるだけで、電子状態は go のものがそのまま使われます。go の積分路の下端 E_F − ewidth_go まで DOS を見るには

- ref = 0.75 なら ewidth_dos ≥ ewidth_go / 0.75
- ref = 0.5 なら ewidth_dos ≥ 2 · ewidth_go
- または `cemesr_ref= 1.0` にして ewidth_dos = ewidth_go

とします。E_F − ewidth_go で DOS が 0 でなく左に裾が続いていれば、go が価電子帯の底を取りこぼしていた、つまり電荷が自己無撞着ではなかったということなので、ewidth_go を広げて go からやり直します。

dos のメッシュ点数（`msex`）は固定なので、ewidth_dos を広げると刻みが粗くなります。価電子帯の底の確認用（広い窓）と E_F 付近を見る用（狭い窓）を分けるのが実用的です。

## example の値

`example/run_examples.py` と `aiida_akaikkr/presets.py` は dos の ewidth を一律 2.0 にし、go の ewidth はテストスクリプトの値（Cu, Fe, Co, Ni, NiFe, Fe_lmd, FeRh05Pt05: 1.0、Co2MnSi: 1.2、AlMnFeCo_bcc: 1.3、FeB195, GaAs: 1.5、SmCo5_oc: 1.7）です。dos の窓は物質ではなく code のビルドで決まり、specx-akaikkr なら [−1.5, +0.5]、specx-cnd なら [−1.0, +1.0] です。specx-cnd では go の ewidth が 1.0 を超える物質で E_F − ewidth_go が窓の外に落ちます。

`aiida_akaikkr.plot` の DOS / PDOS 図は E − E_F = −ewidth_go（`inputs.potential.creator` の go の ewidth）に一点鎖線を引き、窓の外なら横軸を線まで広げます（[examples.md](examples.md) §4）。

## EW, EZ

出力の `ew=`, `ez=` は ewidth とは別物です。Green 関数に要る Wronskian と動径波動関数はエネルギー EW ± EZ の範囲のサンプリング点で計算してチェビシェフ多項式で近似するので、積分路も dos のメッシュも EW ± EZ に収まっていなければなりません。EW は窓の中心、EZ は半幅で、`spmain.f` が積分路の上端 `efsp` と下端 `efif` から

```
ew = (efsp + efif) / 2
ez = (efsp − efif) / 2 + emrgn      (emrgn = 0.7 Ry)
```

と自動で作り、反復中に E_F が動いて範囲を外れると作り直します（`***msg in spmain...new ew, ez generated`）。原点はマフィンティン球間の一定ポテンシャル（マニュアル図 6）で、E_F からの相対値ではありません。マニュアルの例 `ew= 0.09998 ez= 0.80300`（ewidth 1.2）は余裕が 0.2 だった旧版の出力で、現行ソースなら同じ入力で ez ≈ 1.3 になります。
