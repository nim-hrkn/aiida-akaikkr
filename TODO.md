# TODO（aiida-akaikkr）

- [ ] **構造生成を ASE 版にする**（2026-09-26、ユーザー指示）。現在 `inputs.make_common_param` は `get_kkr_struc_from_cif`（pymatgen / CIF backend、`backend: "cif"`）で AkaiKKR の構造パラメタを作り、GAES は `single_site_common_param` で構造を使わない。`pyakaikkr.ase`（`get_kkr_struc_from_ase`、`backend: "ase"`、type の並びは `type_order="electronegativity"` で pymatgen と一致）に切り替える。注意点: pymatgen → ASE は部分占有を受け付けない（CIF を `ase.io.read` するか `pyakaikkr.ase.set_occupancy`）、rmt=0 の自動決定が type / 原子の並びに依存する（並びを変えると参照と合わない）、type 名が backend で違う（pymatgen `Fe0.1Ni0.9_4a_0`、ASE は番号）。参照比較（`compare_reference.py`）と 13 物質の例で結果が変わらないことを確認してから既定にする。構造生成にローカルの specx（`local_specx_for`）が要る点は ASE 版でも同じ（geometry モードで一度走らせる）。
- [ ] `make_common_param` に `source`（POSCAR 名など）を残し、レポートの「構造の出典」に出す。
- [ ] preset の構造生成を specx 無しで行う（geometry モードの代替）。
