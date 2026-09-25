"""assemble docs/intro_ja.html (beginner's introduction, self-contained, inline SVG) from the SVG sources next to this script:
2682.dot.svg (verdi node graph generate 2682), atomic_levels.svg (tests/gaes/tools/plot_atomic_levels.py), alscnibi.svg and
se4s_valence_core.svg (tests/gaes/tools/plot_orbital_rules.py on the tests/akaikkr GAES examples). usage: python build_intro.py [out.html]"""
import html
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.normpath(os.path.join(HERE, "..", "..", "intro_ja.html"))   # docs/intro_ja.html


def svg(name, max_width="100%"):
    t = open(os.path.join(HERE, name), encoding="utf-8").read()
    i = t.find("<svg")
    t = t[i:]
    # make it responsive: drop fixed width/height, keep viewBox
    t = re.sub(r'<svg([^>]*?)\swidth="[^"]+"', r'<svg\1', t, count=1)
    t = re.sub(r'<svg([^>]*?)\sheight="[^"]+"', r'<svg\1', t, count=1)
    if "viewBox" not in t[:400]:
        m = re.search(r'width="([\d.]+)pt" height="([\d.]+)pt"', open(os.path.join(HERE, name), encoding="utf-8").read())
        if m:
            t = t.replace("<svg", '<svg viewBox="0 0 {} {}"'.format(m.group(1), m.group(2)), 1)
    return '<div class="fig" style="max-width:{}">{}</div>'.format(max_width, t)


def code(s):
    return "<pre><code>{}</code></pre>".format(html.escape(s))


CSS = """
body{font-family:-apple-system,"Segoe UI",Roboto,"Noto Sans JP","Hiragino Sans",sans-serif;color:#0b0b0b;max-width:1000px;margin:2em auto;padding:0 1.5em;line-height:1.7}
h1{font-size:1.6em;border-bottom:2px solid #d9d8d3;padding-bottom:.3em}h2{font-size:1.2em;margin-top:2.2em;color:#2a4a7a;border-left:6px solid #2a78d6;padding-left:.5em}
h3{font-size:1.05em;margin-top:1.4em}p,li{font-size:1em}pre{background:#f4f4f1;border:1px solid #d9d8d3;padding:.7em 1em;overflow-x:auto;font-size:.9em}
code{font-family:ui-monospace,Menlo,Consolas,monospace}table{border-collapse:collapse;margin:.6em 0 1em}th,td{border:1px solid #d9d8d3;padding:.3em .7em;text-align:left;font-size:.95em}th{background:#f4f4f1}
.fig{margin:1em auto}.fig svg{width:100%;height:auto;border:1px solid #eee;background:#fff}.cap{font-size:.9em;color:#52514e;margin:-.3em 0 1.4em}
.arch{font-family:ui-monospace,Menlo,Consolas,monospace;white-space:pre;background:#f4f4f1;border:1px solid #d9d8d3;padding:1em;font-size:.95em;line-height:1.5}
.say{background:#eef5ff;border-left:4px solid #2a78d6;padding:.5em .9em;margin:.6em 0}.say b{color:#2a4a7a}
.note{background:#fff7e6;border-left:4px solid #eda100;padding:.5em .9em;margin:.8em 0}
a{color:#2a4a7a}.meta{color:#52514e;font-size:.85em;margin-top:3em}
"""

ARCH = """Claude（LLM） ── MCP ── aiida-akaikkr ── AiiDA ── 計算機（SLURM / ssh）
                              │                        │
                              └── pyakaikkr ───────── AkaiKKR (specx)
                                   入力を書く / 出力を読む / 図を描く"""

import json as _json
_ex = _json.load(open(os.path.join(HERE, "smco5_example.json"))) if os.path.exists(os.path.join(HERE, "smco5_example.json")) else {}
def _fill(t):
    for k, v in _ex.items():
        t = t.replace("{" + k + "}", str(v))
    return t
page = []
page.append("""<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8"><title>aiida-akaikkr 入門</title><style>{}</style></head><body>""".format(CSS))
page.append("<h1>aiida-akaikkr 入門 — AkaiKKR を AiiDA と LLM から使う</h1>")
page.append("""<p><b>AkaiKKR（specx）</b>は KKR-CPA 法の第一原理計算プログラムで、結晶と混晶（CPA）の電子状態、
状態密度（DOS）、ブロッホスペクトル関数 A(ω,k)、交換相互作用 J<sub>ij</sub> とキュリー温度、電気伝導（cnd）を計算します。
<b>aiida-akaikkr</b> は、それを <a href="https://www.aiida.net/">AiiDA</a> のワークフロー基盤の上で動かすためのプラグインです。
計算の投入・監視・結果の取り出しをコマンド（CLI）と MCP ツールで提供し、Claude のような LLM から自然言語で操作できます。
入力の生成と出力の解析、図の描画は Python ライブラリ <b>pyakaikkr</b>（AkaiKKRPythonUtil）が担当します。</p>""")

page.append("<h2>構成</h2>")
page.append(svg("architecture.svg", max_width="980px"))
page.append('<p class="cap">図 0: 構成。人は Claude に日本語で指示し、Claude が MCP ツールを呼ぶ。aiida-akaikkr が AiiDA を通して計算機に specx のジョブを投げ、pyakaikkr が入出力と図を担当する。</p>')
page.append("""<table><tr><th>層</th><th>役割</th></tr>
<tr><td>Claude + MCP</td><td>「Cu の DOS を計算して」のような指示を、MCP ツール <code>kkr_submit_chain</code> などの呼び出しに変える</td></tr>
<tr><td>aiida-akaikkr</td><td>AkaiKKR の各モード（go, dos, spc, jij, tc, cnd, fsm）を AiiDA の CalcJob に、go → 後続 や ewidth の自動決定（GAES）を WorkChain にする。CLI <code>akaikkr-aiida</code>、MCP <code>akaikkr-mcp</code></td></tr>
<tr><td>AiiDA</td><td>ジョブの投入（SLURM、ssh）、入出力の保存、<b>来歴（provenance）</b>の記録</td></tr>
<tr><td>pyakaikkr</td><td>inputcard の生成、out_*.log の解析、DOS / A(ω,k) / J<sub>ij</sub> の描画、GAES の判定、HTML レポート</td></tr></table>""")

page.append("<h2>利点 1: AiiDA が backend なので来歴（provenance）が残る</h2>")
page.append("""<p>どの構造（CIF）から、どの code とパラメタで go を回し、そのポテンシャルからどの dos / spc / jij / cnd を計算したかが、
すべてノードとリンクとして保存されます。半年後に「この J<sub>ij</sub> はどの ewidth で計算した？」と聞かれても、pk ひとつで辿れます。
下は FeRh<sub>0.5</sub>Pt<sub>0.5</sub> の go（pk 2682）を中心にした来歴図です（<code>verdi node graph generate 2682</code>）。
上が入力（構造の Dict、code、magtyp）、下が出力（ポテンシャル、構造、結果）と、そのポテンシャルを使った後続の計算です。</p>""")
page.append(svg("2682.dot.svg"))
page.append('<p class="cap">図 1: FeRh0.5Pt0.5 の go とその後続（dos / fsm / cnd / tc / jij / spc）の来歴。赤が計算、緑がデータ、矢印がリンク。</p>')
page.append('<div class="say"><b>Claude に:</b> 「pk 2718 の J<sub>ij</sub> はどの計算から来たか教えて」 → <code>kkr_provenance(pk=2718)</code></div>')

page.append("<h2>背景: KKR 法と ewidth</h2>")
page.append("""<p>KKR 法（他の多体グリーン関数法も同じ）では、価電子数をフェルミエネルギー E<sub>F</sub> から下へ幅 <b>ewidth</b> の
複素エネルギー積分路で数えます。積分路の下端 E<sub>F</sub> − ewidth より下の状態は core（原子内に閉じた状態）として扱われ、上の状態は価電子として自己無撞着に解かれます。
下端が価電子帯の中や、In 4d・Bi 5d・Sn 4d のような <b>semicore</b>（浅い内殻準位）の上に乗ると、SCF が収束しない、あるいは収束しても電子数が合わない結果になります。
semicore を持つ元素が混じった合金では、元素ごとに準位の深さが違うため、ewidth の手動設定が面倒でした。</p>""")
page.append(svg("atomic_levels.svg"))
page.append('<p class="cap">図 2: 元素 H〜Bi の原子の core 準位（specx の go=dsp、単体 fcc、原子ポテンシャルから）。右軸は E<sub>F</sub> = 0.6 Ry としたときの E − E<sub>F</sub>。E<sub>F</sub> − 1.2 Ry の破線付近に準位を持つ元素（Ga 3d、In 4d、Tl 5d、Pb 5d、Bi 6s、希土類の 5p など）で ewidth の設定が問題になる。</p>')

page.append("<h2>利点 2: GAES — ewidth を自動で決める</h2>")
page.append("""<p><b>GAES</b>（Gap-Anchored Ewidth Search）は、go → dos を繰り返し、DOS が閾値より小さい連続した区間（バンドギャップ）に
積分路の下端が入るまで ewidth を動かします。DOS の微分は使わず、判定は DOS の値だけです。ewidth の探索範囲 [min, max] を与えると、その中で決めます。</p>
<ul><li><b>範囲を指定して自動決定</b>: <code>kkr_submit_gaes(comp="AlScNiBi", polytyp="fcc", min_ewidth=1.0, max_ewidth=1.5)</code></li>
<li><b>元素と軌道を指定</b>: 「Se の 4s を価電子（valence）にする」「core にする」と言うと、その準位を go の出力から読み、ewidth の範囲に変換します
（valence → min = |E − E<sub>F</sub>| + 0.2、core → max = |E − E<sub>F</sub>| − 0.2）。<code>kkr_submit_gaes(comp="SeMnFeCo", orbital="Se4s=core")</code></li></ul>""")
page.append("<h3>例: AlScNiBi fcc、ewidth を [1.0, 1.5] で決める</h3>")
page.append(svg("alscnibi.svg"))
page.append('<p class="cap">図 3: AlScNiBi（等比、単一サイト CPA）の GAES。緑 / 青 = 粗い / 細かいギャップ区間、斜線 = 指定した範囲 [1.0, 1.5]、赤 = 決まった −ewidth、青線 = 成分の core 準位（破線は valence 扱い）。</p>')
page.append("<h3>例: SeMnFeCo fcc、Se 4s を valence / core に切り替える</h3>")
page.append(svg("se4s_valence_core.svg"))
page.append('<p class="cap">図 4: 左は Se4s=valence（4s 帯の下のギャップに落ち着く、ewidth 1.42）、右は Se4s=core（4s と価電子帯の間、ewidth 0.77）。同じ軌道でも扱いによって準位の位置も動く（−1.04 → −1.05 Ry）。</p>')
page.append('<div class="say"><b>Claude に:</b> 「SeMnFeCo の fcc を Se 4s を core にして GAES で計算して」 → <code>kkr_submit_gaes(comp="SeMnFeCo", polytyp="fcc", orbital="Se4s=core", code="specx-akaikkr@mygardenx2-slurm")</code>、終わったら <code>kkr_results(pk)</code> と <code>kkr_plot(gaes_pk=pk)</code></div>')

page.append("<h2>利点 3: レポートが出せる</h2>")
page.append("""<p>1 つの計算（go とその後続）の要約を、図付きの 1 枚の HTML にできます。組成式、構造の出典（CIF / POSCAR / preset）、空間群、
計算条件、SCF の結果（E<sub>F</sub>、全エネルギー、全モーメント）、成分ごとのモーメントと電荷、DOS / PDOS / A(ω,k) / J<sub>ij</sub>（キュリー温度）/ GAES の図、cnd の値、来歴。日本語と英語。</p>
<p>例: <a href="data/report_SmCo5_oc_ja.html">SmCo5（open core）のレポート</a>、<a href="data/report_FeRh05Pt05_aiida_ja.html">FeRh0.5Pt0.5（AiiDA、図 1 の計算）のレポート</a>、<a href="data/report_Cu_gaes_aiida_ja.html">Cu の GAES のレポート</a>。</p>""")
page.append('<div class="say"><b>Claude に:</b> 「pk 2682 のレポートを日本語で作って」 → <code>kkr_report(pk=2682, lang="ja")</code>（HTML のパスと要約が返る）</div>')

page.append("<h2>利点 4: LLM 経由なので、人にとって操作が楽</h2>")
page.append("""<p>inputcard の書式、AiiDA の <code>verdi</code> コマンド、pk の追い方を覚えなくても、やりたいことを日本語で言えば Claude が対応する MCP ツールを選び、
順に呼び、結果を読んで説明します。ツールは読み取り（状態、結果、図、来歴、レポート）と投入（go、後続、chain、GAES）に分かれていて、投入は起動時に許可した場合だけ使えます。</p>
<h3>実際の指示例</h3>
<div class="say"><b>人:</b> 「ファイル structure/SmCo5_P6mmm.cif を読んで、GAES で ewidth を決めて、J<sub>ij</sub> を計算し、レポートを出せ」</div>
<p>Claude はこの 1 文を次の 4 段に分けて実行します（2026-09-26 に実際に行った例。pk はそのときの値）。</p>
<table><tr><th>段</th><th>Claude が呼ぶツール</th><th>返るもの</th></tr>
<tr><td>1. CIF を読む</td><td><code>kkr_structure_from_cif(cif_path="structure/SmCo5_P6mmm.cif", code="specx-akaikkr@mygardenx2-slurm", magtype="mag")</code></td><td>構造の Dict pk {STRUCTURE_PK}（P6/mmm、6 原子、type Sm_1a_0 / Co_3g_1 / Co_2c_4）</td></tr>
<tr><td>2. ewidth を決める</td><td><code>kkr_submit_gaes(structure_pk={STRUCTURE_PK}, code=..., ncores=12)</code> → <code>kkr_wait(pk)</code> を繰り返す</td><td>GAES WorkChain pk {GAES_PK}: {GAES_RESULT}</td></tr>
<tr><td>3. J<sub>ij</sub> を計算</td><td><code>kkr_submit_followup(go_pk={GO_PK}, mode="jij")</code> → <code>kkr_wait</code></td><td>jij CalcJob pk {JIJ_PK}: {JIJ_RESULT}</td></tr>
<tr><td>4. レポート</td><td><code>kkr_report(pk={GAES_PK}, jij_pk={JIJ_PK}, lang="ja")</code></td><td><a href="data/report_SmCo5_gaes_ja.html">HTML レポート</a>（式、空間群、SCF、成分表、DOS、J<sub>ij</sub>、GAES の図）</td></tr></table>
<p>人が書いたのは最初の 1 文だけです。CLI で同じことをすると次の 4 コマンドになります。</p>
""")
page.append(code("""akaikkr-aiida structure --cif-path structure/SmCo5_P6mmm.cif --code specx-akaikkr@mygardenx2-slurm --magtype mag   # -> pk {STRUCTURE_PK}
akaikkr-aiida submit-gaes --structure-pk {STRUCTURE_PK} --code specx-akaikkr@mygardenx2-slurm --ncores 12            # -> pk {GAES_PK}
akaikkr-aiida submit-followup --go-pk {GO_PK} --mode jij                                                             # -> pk {JIJ_PK}
akaikkr-aiida report --pk {GAES_PK} --jij-pk {JIJ_PK} --lang ja"""))
page.append("""<table><tr><th>他の言い方</th><th>Claude が呼ぶもの</th></tr>
<tr><td>「Cu の DOS とバンド分散を計算して」</td><td><code>kkr_submit_chain(preset="Cu", modes="dos,spc")</code></td></tr>
<tr><td>「今走っている計算は？」</td><td><code>kkr_list()</code>、<code>kkr_process(pk)</code></td></tr>
<tr><td>「pk 2682 の結果を見せて」</td><td><code>kkr_results(pk=2682)</code></td></tr>
<tr><td>「SeMnFeCo の Se 4s を core にして ewidth を決めて」</td><td><code>kkr_submit_gaes(comp="SeMnFeCo", polytyp="fcc", orbital="Se4s=core")</code></td></tr></table>
<div class="note">全部の操作は CLI <code>akaikkr-aiida</code> でも同じにできます（MCP は CLI を呼ぶ薄い層です）。行動記録は <code>~/.aiida-akaikkr/log/</code> に残ります。</div>""")
page.append("<h2>動かすまで</h2>")
page.append(code("""# 環境（conda env akaikkr）: aiida-core 2.9、RabbitMQ、pyakaikkr、aiida-akaikkr
pip install -e AkaiKKRPythonUtil/library/PyAkaiKKR    # pyakaikkr
pip install -e aiida-akaikkr                           # プラグイン（entry point akaikkr.*）
verdi presto --profile-name akaikkr                    # プロファイル
verdi computer setup ... / verdi code create ...       # 計算機（SLURM、ssh）と specx の登録
verdi daemon start
akaikkr-aiida status                                   # 動作確認
claude mcp add akaikkr -- /path/to/env/bin/akaikkr-mcp --allow-submit   # Claude に登録"""))

page.append("<h2>知っておくこと</h2>")
page.append("""<ul>
<li>ewidth をわずかに変えるだけで SCF の収束の可否が反転することがあります。収束したことは積分路がギャップにある証拠にはならず、逆も同じです。判定は DOS で行います。</li>
<li>多原子胞ではギャップ判定の DOS を原子あたりに直します（既定）。1 原子の CPA 用に決めた閾値をそのまま使うためです。</li></ul>""")
page.append('<p class="meta">aiida-akaikkr v1.0.0 / pyakaikkr v1.0.0（2026-09-26）。図は inline SVG（来歴図は AiiDA の graphviz 出力、その他は pyakaikkr.plot）。</p>')
page.append("</body></html>")
open(OUT, "w", encoding="utf-8").write(_fill("\n".join(page)))
print("wrote", OUT, "%.0f KB" % (os.path.getsize(OUT) / 1024))
