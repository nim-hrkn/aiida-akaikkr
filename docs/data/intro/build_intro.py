"""assemble docs/intro.html: beginner's introduction to aiida-akaikkr, one self-contained file, Japanese and
English selectable with a switch at the top (the choice is remembered in the browser), table of contents,
inline SVG figures, the SmCo5 report appended at the end.

SVG sources next to this script: architecture.svg (hand drawn; the English labels are substituted here),
2682.dot.svg (verdi node graph generate 2682), atomic_levels.svg (tests/gaes/tools/plot_atomic_levels.py),
alscnibi.svg and se4s_valence_core.svg (tests/gaes/tools/plot_orbital_rules.py on the tests/akaikkr GAES examples).
smco5_example.json holds the pks / results of the worked example. usage: python build_intro.py [out.html]
"""
import html
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.normpath(os.path.join(HERE, "..", "..", "intro.html"))
REPORT_JA = os.path.normpath(os.path.join(HERE, "..", "report_SmCo5_gaes_ja.html"))
REPORT_EN = os.path.normpath(os.path.join(HERE, "..", "report_SmCo5_gaes_en.html"))
EX = json.load(open(os.path.join(HERE, "smco5_example.json"), encoding="utf-8")) if os.path.exists(os.path.join(HERE, "smco5_example.json")) else {}

ARCH_EN = {"LLM（人が日本語で指示）": "LLM (the person gives instructions)", "来歴・投入・保存": "provenance, submission, storage",
           "計算機": "computer", "SLURM（local / ssh）で specx": "specx under SLURM (local / ssh)",
           "inputcard 生成・out_*.log 解析・図・GAES・レポート": "inputcard, out_*.log parsing, figures, GAES, report",
           "入出力の処理を委ねる": "delegates input / output handling", "インストール済みの specx を実行": "runs the installed specx",
           "実線: 呼び出し、破線: 計算機にあらかじめインストールされた specx をジョブとして実行する。人は左端の Claude にだけ話す。":
           "solid: calls; dashed: specx installed on the computer runs as a job. The person talks only to Claude at the left."}


def svg(name, max_width="100%", translate=None):
    t = open(os.path.join(HERE, name), encoding="utf-8").read()
    t = t[t.find("<svg"):]
    if translate:
        for ja, en in translate.items():
            t = t.replace(ja, en)
    t = re.sub(r'<svg([^>]*?)\swidth="[^"]+"', r'<svg\1', t, count=1)
    t = re.sub(r'<svg([^>]*?)\sheight="[^"]+"', r'<svg\1', t, count=1)
    if "viewBox" not in t[:400]:
        m = re.search(r'width="([\d.]+)pt" height="([\d.]+)pt"', open(os.path.join(HERE, name), encoding="utf-8").read())
        if m:
            t = t.replace("<svg", '<svg viewBox="0 0 {} {}"'.format(m.group(1), m.group(2)), 1)
    return '<div class="fig" style="max-width:{}">{}</div>'.format(max_width, t)


def code(s):
    return "<pre><code>{}</code></pre>".format(html.escape(s))


def fill(t):
    for k, v in EX.items():
        t = t.replace("{" + k + "}", str(v))
    return t


CSS = """
body{font-family:-apple-system,"Segoe UI",Roboto,"Noto Sans JP","Hiragino Sans",sans-serif;color:#0b0b0b;max-width:1000px;margin:2em auto;padding:0 1.5em;line-height:1.7}
h1{font-size:1.6em;border-bottom:2px solid #d9d8d3;padding-bottom:.3em}h2{font-size:1.2em;margin-top:2.2em;color:#2a4a7a;border-left:6px solid #2a78d6;padding-left:.5em}
h3{font-size:1.05em;margin-top:1.4em}p,li{font-size:1em}pre{background:#f4f4f1;border:1px solid #d9d8d3;padding:.7em 1em;overflow-x:auto;font-size:.9em}
code{font-family:ui-monospace,Menlo,Consolas,monospace}table{border-collapse:collapse;margin:.6em 0 1em}th,td{border:1px solid #d9d8d3;padding:.3em .7em;text-align:left;font-size:.95em;vertical-align:top}th{background:#f4f4f1}
.fig{margin:1em auto}.fig svg{width:100%;height:auto;border:1px solid #eee;background:#fff}.cap{font-size:.9em;color:#52514e;margin:-.3em 0 1.4em}
.say{background:#eef5ff;border-left:4px solid #2a78d6;padding:.5em .9em;margin:.6em 0}.say b{color:#2a4a7a}
.note{background:#fff7e6;border-left:4px solid #eda100;padding:.5em .9em;margin:.8em 0}
a{color:#2a4a7a}.meta{color:#52514e;font-size:.85em;margin-top:3em}
.langbar{position:sticky;top:0;background:#fff;padding:.4em 0;border-bottom:1px solid #d9d8d3;z-index:5}
.langbar button{font:inherit;padding:.2em .9em;margin-right:.4em;border:1px solid #2a4a7a;background:#fff;color:#2a4a7a;border-radius:4px;cursor:pointer}
.langbar button.on{background:#2a4a7a;color:#fff}
nav.toc{background:#f4f4f1;border:1px solid #d9d8d3;padding:.6em 1.2em;margin:1em 0}nav.toc ol{margin:.3em 0;padding-left:1.4em}
.report{border:2px solid #d9d8d3;padding:0 1.2em 1em;margin-top:1em;background:#fcfcfa}.report h1{font-size:1.25em;border:none}.report h2{font-size:1.05em;border-left-color:#eda100;color:#0b0b0b}
.report figure{margin:1.2em 0}.report figure svg,.report figure img{max-width:100%;height:auto;border:1px solid #eee}.report figcaption{font-size:.9em;color:#52514e}.report td.num{text-align:right;font-variant-numeric:tabular-nums}
"""

JS = """
function setLang(l){document.documentElement.lang=l;
document.querySelectorAll('.lang').forEach(function(e){e.style.display=e.classList.contains(l)?'':'none';});
document.querySelectorAll('.langbar button').forEach(function(b){b.classList.toggle('on',b.dataset.lang===l);});
try{localStorage.setItem('intro_lang',l);}catch(e){}}
document.addEventListener('DOMContentLoaded',function(){var l='ja';try{l=localStorage.getItem('intro_lang')||'ja';}catch(e){}
if(location.hash==='#en')l='en';if(location.hash==='#ja')l='ja';setLang(l);});
"""

# ---------------------------------------------------------------- content: list of (id, {ja: html, en: html}) plus shared figures
S = []       # page pieces in order


def both(ja, en):
    S.append('<div class="lang ja">{}</div><div class="lang en">{}</div>'.format(ja, en))


def shared(x):
    S.append(x)


SECTIONS = [("arch", "構成", "Architecture"), ("prov", "利点 1: 来歴が残る", "Benefit 1: provenance is kept"),
            ("bg", "背景: KKR 法と ewidth", "Background: the KKR method and ewidth"), ("gaes", "利点 2: GAES で ewidth を自動決定", "Benefit 2: GAES decides ewidth"),
            ("report", "利点 3: レポートが出せる", "Benefit 3: reports"), ("llm", "利点 4: LLM 経由なので、人にとって操作が楽", "Benefit 4: operation through an LLM is easy for people"),
            ("setup", "動かすまで", "Getting started"), ("notes", "知っておくこと", "Things to know"), ("appendix", "付録: SmCo5 のレポート", "Appendix: the SmCo5 report")]


def h2(sid, ja, en):
    both('<h2 id="{0}">{1}</h2>'.format(sid, ja), '<h2 id="{0}-en">{1}</h2>'.format(sid, en))


# title + intro
both("<h1>aiida-akaikkr 入門 — AkaiKKR を AiiDA と LLM から使う</h1>", "<h1>Introduction to aiida-akaikkr — AkaiKKR through AiiDA and an LLM</h1>")
both("""<p><b>AkaiKKR（specx）</b>は KKR-CPA 法の第一原理計算プログラムで、結晶と混晶（CPA）の電子状態、状態密度（DOS）、ブロッホスペクトル関数 A(ω,k)、
交換相互作用 J<sub>ij</sub> とキュリー温度、電気伝導（cnd）を計算します。<b>aiida-akaikkr</b> は、それを AiiDA のワークフロー基盤の上で動かすためのプラグインです。
計算の投入・監視・結果の取り出しをコマンド（CLI）と MCP ツールで提供し、Claude のような LLM から自然言語で操作できます。
入力の生成と出力の解析、図の描画は Python ライブラリ <b>pyakaikkr</b>（AkaiKKRPythonUtil）が担当します。</p>""",
     """<p><b>AkaiKKR (specx)</b> is a first-principles code based on the KKR-CPA method: electronic structure of crystals and alloys (CPA),
density of states (DOS), Bloch spectral function A(ω,k), exchange couplings J<sub>ij</sub> with the Curie temperature, and electrical conductivity (cnd).
<b>aiida-akaikkr</b> is the plugin that runs it on the AiiDA workflow platform. It provides submission, monitoring and retrieval of results as
command-line and MCP tools, so that an LLM such as Claude can operate it in natural language. Input generation, output parsing and figures are
the job of the Python library <b>pyakaikkr</b> (AkaiKKRPythonUtil).</p>""")
# toc
toc_ja = '<nav class="toc"><b>目次</b><ol>' + "".join('<li><a href="#{}">{}</a></li>'.format(sid, ja) for sid, ja, en in SECTIONS) + "</ol></nav>"
toc_en = '<nav class="toc"><b>Contents</b><ol>' + "".join('<li><a href="#{}-en">{}</a></li>'.format(sid, en) for sid, ja, en in SECTIONS) + "</ol></nav>"
both(toc_ja, toc_en)

# architecture
h2("arch", "構成", "Architecture")
both(svg("architecture.svg", "980px"), svg("architecture.svg", "980px", translate=ARCH_EN))
both('<p class="cap">図 0: 構成。人は Claude に日本語で指示し、Claude が MCP ツールを呼ぶ。aiida-akaikkr が AiiDA を通して計算機にジョブを投げ、計算機にインストール済みの specx が動く。pyakaikkr が入出力と図を担当する。</p>',
     '<p class="cap">Fig. 0: architecture. The person instructs Claude; Claude calls MCP tools; aiida-akaikkr submits jobs through AiiDA to the computer, where the installed specx runs; pyakaikkr handles inputs, outputs and figures.</p>')
both("""<table><tr><th>層</th><th>役割</th></tr>
<tr><td>Claude + MCP</td><td>「Cu の DOS を計算して」のような指示を、MCP ツール <code>kkr_submit_chain</code> などの呼び出しに変える</td></tr>
<tr><td>aiida-akaikkr</td><td>AkaiKKR の各モード（go, dos, spc, jij, tc, cnd, fsm）を AiiDA の CalcJob に、go → 後続 や ewidth の自動決定（GAES）を WorkChain にする。CLI <code>akaikkr-aiida</code>、MCP <code>akaikkr-mcp</code></td></tr>
<tr><td>AiiDA</td><td>ジョブの投入（SLURM、ssh）、入出力の保存、<b>来歴（provenance）</b>の記録</td></tr>
<tr><td>pyakaikkr</td><td>inputcard の生成、out_*.log の解析、DOS / A(ω,k) / J<sub>ij</sub> の描画、GAES の判定、HTML レポート</td></tr></table>""",
     """<table><tr><th>Layer</th><th>Role</th></tr>
<tr><td>Claude + MCP</td><td>turns an instruction such as "compute the DOS of Cu" into calls of MCP tools like <code>kkr_submit_chain</code></td></tr>
<tr><td>aiida-akaikkr</td><td>wraps every AkaiKKR mode (go, dos, spc, jij, tc, cnd, fsm) as an AiiDA CalcJob, and go → follow-ups or the automatic ewidth search (GAES) as WorkChains. CLI <code>akaikkr-aiida</code>, MCP <code>akaikkr-mcp</code></td></tr>
<tr><td>AiiDA</td><td>job submission (SLURM, ssh), storage of inputs and outputs, <b>provenance</b></td></tr>
<tr><td>pyakaikkr</td><td>inputcard generation, parsing of out_*.log, DOS / A(ω,k) / J<sub>ij</sub> figures, the GAES judgement, HTML reports</td></tr></table>""")

# benefit 1
h2("prov", "利点 1: AiiDA が backend なので来歴（provenance）が残る", "Benefit 1: AiiDA as the backend keeps the provenance")
both("""<p>どの構造（CIF）から、どの code とパラメタで go を回し、そのポテンシャルからどの dos / spc / jij / cnd を計算したかが、すべてノードとリンクとして保存されます。
半年後に「この J<sub>ij</sub> はどの ewidth で計算した？」と聞かれても、pk ひとつで辿れます。下は FeRh<sub>0.5</sub>Pt<sub>0.5</sub> の go（pk 2682）を中心にした来歴図です
（<code>verdi node graph generate 2682</code>）。上が入力（構造の Dict、code、magtyp）、下が出力（ポテンシャル、構造、結果）と、そのポテンシャルを使った後続の計算です。</p>""",
     """<p>Which structure (CIF), which code and parameters ran the go, and which dos / spc / jij / cnd used that potential are all stored as nodes and links.
Asked half a year later "which ewidth was this J<sub>ij</sub> computed with?", one pk is enough to trace it. Below is the provenance graph around the go of
FeRh<sub>0.5</sub>Pt<sub>0.5</sub> (pk 2682, <code>verdi node graph generate 2682</code>): inputs at the top (structure Dict, code, magtyp), outputs at the bottom
(potential, structure, results) and the follow-up calculations that used the potential.</p>""")
shared(svg("2682.dot.svg"))
both('<p class="cap">図 1: FeRh0.5Pt0.5 の go とその後続（dos / fsm / cnd / tc / jij / spc）の来歴。赤が計算、緑がデータ、矢印がリンク。</p>',
     '<p class="cap">Fig. 1: provenance of the FeRh0.5Pt0.5 go and its follow-ups (dos / fsm / cnd / tc / jij / spc). Red: calculations, green: data, arrows: links.</p>')
both('<div class="say"><b>Claude に:</b> 「pk 2718 の J<sub>ij</sub> はどの計算から来たか教えて」 → <code>kkr_provenance(pk=2718)</code></div>',
     '<div class="say"><b>To Claude:</b> "which calculation did the J<sub>ij</sub> of pk 2718 come from?" → <code>kkr_provenance(pk=2718)</code></div>')

# background
h2("bg", "背景: KKR 法と ewidth", "Background: the KKR method and ewidth")
both("""<p>KKR 法（他の多体グリーン関数法も同じ）では、価電子数をフェルミエネルギー E<sub>F</sub> から下へ幅 <b>ewidth</b> の複素エネルギー積分路で数えます。
積分路の下端 E<sub>F</sub> − ewidth より下の状態は core（原子内に閉じた状態）として扱われ、上の状態は価電子として自己無撞着に解かれます。
下端が価電子帯の中や、In 4d・Bi 5d・Sn 4d のような <b>semicore</b>（浅い内殻準位）の上に乗ると、SCF が収束しない、あるいは収束しても電子数が合わない結果になります。
semicore を持つ元素が混じった合金では、元素ごとに準位の深さが違うため、ewidth の手動設定が面倒でした。</p>""",
     """<p>The KKR method (like other many-body Green's-function methods) counts the valence electrons by a complex energy contour of width <b>ewidth</b>
below the Fermi energy E<sub>F</sub>. States below the bottom of the contour, E<sub>F</sub> − ewidth, are treated as core (closed atomic states); states above it are
solved self-consistently as valence. When the bottom lies inside the valence band or on top of a <b>semicore</b> level (shallow inner shells such as In 4d, Bi 5d, Sn 4d),
the SCF does not converge, or converges with a wrong electron count. In alloys mixing elements with semicore levels at different depths, setting ewidth by hand was tedious.</p>""")
shared(svg("atomic_levels.svg"))
both('<p class="cap">図 2: 元素 H〜Bi の原子の core 準位（specx の go=dsp、単体 fcc、原子ポテンシャルから）。右軸は E<sub>F</sub> = 0.6 Ry としたときの E − E<sub>F</sub>。E<sub>F</sub> − 1.2 Ry の破線付近に準位を持つ元素（Ga 3d、In 4d、Tl 5d、Pb 5d、Bi 6s、希土類の 5p など）で ewidth の設定が問題になる。</p>',
     '<p class="cap">Fig. 2: atomic core levels of H to Bi (specx go=dsp, elemental fcc, atomic starting potential). Right axis: E − E<sub>F</sub> for E<sub>F</sub> = 0.6 Ry. Elements with a level near the dashed line at E<sub>F</sub> − 1.2 Ry (Ga 3d, In 4d, Tl 5d, Pb 5d, Bi 6s, the rare-earth 5p, ...) are the ones where ewidth matters.</p>')

# benefit 2
h2("gaes", "利点 2: GAES — ewidth を自動で決める", "Benefit 2: GAES — ewidth decided automatically")
both("""<p><b>GAES</b>（Gap-Anchored Ewidth Search）は、ewidth を自動で決める手順です。ある ewidth で自己無撞着（SCF）計算をして状態密度（DOS）を求め、
E<sub>F</sub> − ewidth の位置が「DOS がほぼゼロの連続した区間（バンドギャップ）」に入っているかを調べます。入っていなければ、ギャップの中に入るように ewidth を変えて
SCF 計算と DOS の計算をやり直します。これをギャップに入るまで繰り返します。判定に使うのは DOS の値だけで、DOS の微分は使いません。
ewidth の探索範囲 [min, max] を与えると、その中で決めます。</p>
<ul><li><b>範囲を指定して自動決定</b>: <code>kkr_submit_gaes(comp="AlScNiBi", polytyp="fcc", min_ewidth=1.0, max_ewidth=1.5)</code></li>
<li><b>元素と軌道を指定</b>: 「Se の 4s を価電子（valence）にする」「core にする」と言うと、SCF 計算の結果からその準位のエネルギー E を読み取り、ewidth の範囲に変換します
（valence → E<sub>F</sub> − ewidth がその準位より 0.2 Ry 下になるように min を決める、core → 0.2 Ry 上になるように max を決める）。<code>kkr_submit_gaes(comp="SeMnFeCo", orbital="Se4s=core")</code></li></ul>""",
     """<p><b>GAES</b> (Gap-Anchored Ewidth Search) is a procedure that decides ewidth automatically. With some ewidth it runs the self-consistent (SCF) calculation and
computes the density of states (DOS), then checks whether E<sub>F</sub> − ewidth lies inside a contiguous energy range where the DOS is practically zero (a band gap).
If not, ewidth is changed so that it falls into the gap, and the SCF and DOS calculations are repeated, until it does. Only the DOS values are used for the judgement,
never their derivatives. Given a search range [min, max], it decides inside it.</p>
<ul><li><b>automatic, within a range</b>: <code>kkr_submit_gaes(comp="AlScNiBi", polytyp="fcc", min_ewidth=1.0, max_ewidth=1.5)</code></li>
<li><b>by element and orbital</b>: saying "treat Se 4s as valence" or "as core" reads the energy E of that level from the SCF result and turns it into the ewidth range
(valence: min is set so that E<sub>F</sub> − ewidth lies 0.2 Ry below the level; core: max is set so that it lies 0.2 Ry above). <code>kkr_submit_gaes(comp="SeMnFeCo", orbital="Se4s=core")</code></li></ul>""")
both("""<p>図 3、図 4 の見方（縦軸は DOS の対数、横軸は E − E<sub>F</sub>）:</p>
<table><tr><th>印</th><th>意味</th></tr>
<tr><td>黒線</td><td>最後の判定に使った DOS（胞あたり）</td></tr>
<tr><td>薄い青緑の帯</td><td>粗い判定のギャップ: DOS が 2×10<sup>−2</sup>（原子あたり）より小さい区間が 0.3 Ry 以上続くところ。ここが ewidth の候補になる</td></tr>
<tr><td>濃い青の帯</td><td>その中で細かい判定も満たす部分: DOS が 10<sup>−3</sup>（原子あたり）より小さい区間。E<sub>F</sub> − ewidth はここに置かれる</td></tr>
<tr><td>斜線の帯</td><td>ewidth の探索範囲 [min, max]。横軸では [E<sub>F</sub> − max, E<sub>F</sub> − min]。この帯の外のギャップは選ばれない</td></tr>
<tr><td>赤の一点鎖線</td><td>決まった ewidth の位置 E<sub>F</sub> − ewidth（SCF 計算の積分路の下端）</td></tr>
<tr><td>青の縦線</td><td>指定した軌道の準位（破線 = valence 扱い、実線 = core 扱い）。灰色は他の core 準位</td></tr>
<tr><td>横の点線</td><td>粗い / 細かい判定の閾値（胞あたりに直した値）</td></tr></table>
<h3>例: AlScNiBi fcc、ewidth を [1.0, 1.5] で決める</h3>""",
     """<p>How to read Figs. 3 and 4 (log DOS against E − E<sub>F</sub>):</p>
<table><tr><th>Mark</th><th>Meaning</th></tr>
<tr><td>black line</td><td>the DOS (per cell) used in the last judgement</td></tr>
<tr><td>light blue-green band</td><td>coarse gap: a range of at least 0.3 Ry where the DOS stays below 2×10<sup>−2</sup> (per atom). The candidates for ewidth lie here</td></tr>
<tr><td>dark blue band</td><td>the part of it that also passes the fine judgement: DOS below 10<sup>−3</sup> (per atom). E<sub>F</sub> − ewidth is placed here</td></tr>
<tr><td>hatched band</td><td>the search range [min, max] of ewidth, i.e. [E<sub>F</sub> − max, E<sub>F</sub> − min] on the axis. Gaps outside it are not chosen</td></tr>
<tr><td>red dash-dotted line</td><td>the chosen ewidth, E<sub>F</sub> − ewidth (the bottom of the SCF energy contour)</td></tr>
<tr><td>blue vertical lines</td><td>the levels of the specified orbitals (dashed = treated as valence, solid = as core); gray: other core levels</td></tr>
<tr><td>horizontal dotted lines</td><td>the coarse / fine thresholds (converted to per cell)</td></tr></table>
<h3>Example: AlScNiBi fcc, ewidth decided within [1.0, 1.5]</h3>""")
shared(svg("alscnibi.svg"))
both('<p class="cap">図 3: AlScNiBi（等比、単一サイト CPA）の GAES。薄い青緑の帯 = 粗い判定のギャップ、濃い青の帯 = 細かい判定も満たす部分、斜線 = 指定した範囲 [1.0, 1.5]（横軸で [−1.5, −1.0]）、赤の一点鎖線 = 決まった E<sub>F</sub> − ewidth。青線 = 成分の core 準位（破線は valence 扱い）。ewidth は 1.2 から始めて 1.2375 に決まった。</p>',
     '<p class="cap">Fig. 3: GAES on AlScNiBi (equiatomic single-site CPA). Light blue-green band = coarse gap, dark blue band = the part passing the fine judgement, hatched = the given range [1.0, 1.5] ([−1.5, −1.0] on the axis), red dash-dotted line = the chosen E<sub>F</sub> − ewidth. Blue lines = core levels of the components (dashed = treated as valence). ewidth started at 1.2 and settled at 1.2375.</p>')
both("<h3>例: SeMnFeCo fcc、Se 4s を valence / core に切り替える</h3>", "<h3>Example: SeMnFeCo fcc, Se 4s switched between valence and core</h3>")
shared(svg("se4s_valence_core.svg"))
both('<p class="cap">図 4: 左は Se4s=valence（斜線の帯が Se 4s 準位より下に置かれ、赤線は 4s 帯の下の濃い青の帯に落ち着く、ewidth 1.42）、右は Se4s=core（斜線の帯が 4s 準位より上に置かれ、赤線は 4s と価電子帯の間の濃い青の帯に入る、ewidth 0.77）。同じ軌道でも扱いによって準位の位置も動く（−1.04 → −1.05 Ry）。印の意味は図 3 と同じ。</p>',
     '<p class="cap">Fig. 4: left Se4s=valence (the hatched range is placed below the Se 4s level and the red line settles in the dark blue band below the 4s band, ewidth 1.42), right Se4s=core (the hatched range is placed above the 4s level and the red line falls into the dark blue band between the 4s and the valence band, ewidth 0.77). The level itself moves with the treatment (−1.04 → −1.05 Ry). Marks as in Fig. 3.</p>')
both('<div class="say"><b>Claude に:</b> 「SeMnFeCo の fcc を Se 4s を core にして GAES で計算して」 → <code>kkr_submit_gaes(comp="SeMnFeCo", polytyp="fcc", orbital="Se4s=core", code="specx-akaikkr@mygardenx2-slurm")</code>、終わったら <code>kkr_results(pk)</code> と <code>kkr_plot(gaes_pk=pk)</code></div>',
     '<div class="say"><b>To Claude:</b> "run GAES on fcc SeMnFeCo with Se 4s as core" → <code>kkr_submit_gaes(comp="SeMnFeCo", polytyp="fcc", orbital="Se4s=core", code="specx-akaikkr@mygardenx2-slurm")</code>, then <code>kkr_results(pk)</code> and <code>kkr_plot(gaes_pk=pk)</code></div>')

# benefit 3
h2("report", "利点 3: レポートが出せる", "Benefit 3: reports")
both("""<p>1 つの計算（go とその後続）の要約を、図付きの 1 枚の HTML にできます。組成式、構造の出典（CIF / POSCAR / preset）、空間群、計算条件、SCF の結果
（E<sub>F</sub>、全エネルギー、全モーメント）、成分ごとのモーメントと電荷、DOS / PDOS / A(ω,k) / J<sub>ij</sub>（キュリー温度）/ GAES の図、cnd の値、来歴。日本語と英語。
利点 4 の例で作った SmCo5 のレポートを、このページの末尾（<a href="#appendix">付録</a>）に付けてあります。</p>""",
     """<p>The summary of one calculation (a go and its follow-ups) becomes a single HTML page with figures: formula, structure source (CIF / POSCAR / preset), space group,
calculation parameters, SCF results (E<sub>F</sub>, total energy, total moment), moments and charges per component, DOS / PDOS / A(ω,k) / J<sub>ij</sub> (Curie temperature) / GAES figures,
cnd values and provenance, in Japanese or English. The SmCo5 report produced in the example of benefit 4 is appended at the end of this page (<a href="#appendix-en">appendix</a>).</p>""")
both('<div class="say"><b>Claude に:</b> 「pk 3799 のレポートを日本語で作って」 → <code>kkr_report(pk=3799, jij_pk=3887, lang="ja")</code>（HTML のパスと要約が返る）</div>',
     '<div class="say"><b>To Claude:</b> "make the report of pk 3799 in English" → <code>kkr_report(pk=3799, jij_pk=3887, lang="en")</code> (returns the HTML path and a summary)</div>')

# benefit 4
h2("llm", "利点 4: LLM 経由なので、人にとって操作が楽", "Benefit 4: operation through an LLM is easy for people")
both("""<p>inputcard の書式、AiiDA の <code>verdi</code> コマンド、pk の追い方を覚えなくても、やりたいことを日本語で言えば Claude が対応する MCP ツールを選び、順に呼び、結果を読んで説明します。
ツールは読み取り（状態、結果、図、来歴、レポート）と投入（go、後続、chain、GAES）に分かれていて、投入は起動時に許可した場合だけ使えます。</p>
<h3>実際の指示例</h3>
<div class="say"><b>人:</b> 「ファイル structure/SmCo5_P6mmm.cif を読んで、GAES で ewidth を決めて、J<sub>ij</sub> を計算し、レポートを出せ」</div>
<p>Claude はこの 1 文を次の 4 段に分けて実行します（2026-09-26 に実際に行った例。pk はそのときの値）。</p>
<table><tr><th>段</th><th>Claude が呼ぶツール</th><th>返るもの</th></tr>
<tr><td>1. CIF を読む</td><td><code>kkr_structure_from_cif(cif_path="structure/SmCo5_P6mmm.cif", code="specx-akaikkr@mygardenx2-slurm", magtype="mag")</code></td><td>構造の Dict pk {STRUCTURE_PK}（P6/mmm、6 原子、type Sm_1a_0 / Co_3g_1 / Co_2c_4）</td></tr>
<tr><td>2. ewidth を決める</td><td><code>kkr_submit_gaes(structure_pk={STRUCTURE_PK}, code=..., ncores=12)</code> → <code>kkr_wait(pk)</code> を繰り返す</td><td>GAES WorkChain pk {GAES_PK}: {GAES_RESULT}</td></tr>
<tr><td>3. J<sub>ij</sub> を計算</td><td><code>kkr_submit_followup(go_pk={GO_PK}, mode="jij")</code> → <code>kkr_wait</code></td><td>jij CalcJob pk {JIJ_PK}: {JIJ_RESULT}</td></tr>
<tr><td>4. レポート</td><td><code>kkr_report(pk={GAES_PK}, jij_pk={JIJ_PK}, lang="ja")</code></td><td>HTML レポート（<a href="#appendix">付録</a>: 式、空間群、SCF、成分表、DOS、J<sub>ij</sub>、GAES の図）</td></tr></table>
<p>人が書いたのは最初の 1 文だけです。CLI で同じことをすると次の 4 コマンドになります。</p>""",
     """<p>Without learning the inputcard format, AiiDA's <code>verdi</code> commands or how to follow pks, the person states the goal in plain language; Claude picks the MCP tools,
calls them in order, reads the results and explains them. The tools are split into read-only ones (status, results, figures, provenance, report) and submitting ones
(go, follow-ups, chains, GAES); submission is available only when allowed at start-up.</p>
<h3>A real instruction</h3>
<div class="say"><b>Person:</b> "read the file structure/SmCo5_P6mmm.cif, decide ewidth with GAES, compute J<sub>ij</sub> and write a report"</div>
<p>Claude carries out this one sentence in four steps (done for real on 2026-09-26; the pks are those of that run).</p>
<table><tr><th>Step</th><th>Tool called by Claude</th><th>Result</th></tr>
<tr><td>1. read the CIF</td><td><code>kkr_structure_from_cif(cif_path="structure/SmCo5_P6mmm.cif", code="specx-akaikkr@mygardenx2-slurm", magtype="mag")</code></td><td>structure Dict pk {STRUCTURE_PK} (P6/mmm, 6 atoms, types Sm_1a_0 / Co_3g_1 / Co_2c_4)</td></tr>
<tr><td>2. decide ewidth</td><td><code>kkr_submit_gaes(structure_pk={STRUCTURE_PK}, code=..., ncores=12)</code> → repeated <code>kkr_wait(pk)</code></td><td>GAES WorkChain pk {GAES_PK}: {GAES_RESULT_EN}</td></tr>
<tr><td>3. compute J<sub>ij</sub></td><td><code>kkr_submit_followup(go_pk={GO_PK}, mode="jij")</code> → <code>kkr_wait</code></td><td>jij CalcJob pk {JIJ_PK}: {JIJ_RESULT_EN}</td></tr>
<tr><td>4. report</td><td><code>kkr_report(pk={GAES_PK}, jij_pk={JIJ_PK}, lang="ja")</code></td><td>HTML report (<a href="#appendix-en">appendix</a>: formula, space group, SCF, components, DOS, J<sub>ij</sub>, GAES figures)</td></tr></table>
<p>The person wrote only the first sentence. The same with the CLI is the following four commands.</p>""")
shared(code("""akaikkr-aiida structure --cif-path structure/SmCo5_P6mmm.cif --code specx-akaikkr@mygardenx2-slurm --magtype mag   # -> pk {STRUCTURE_PK}
akaikkr-aiida submit-gaes --structure-pk {STRUCTURE_PK} --code specx-akaikkr@mygardenx2-slurm --ncores 12            # -> pk {GAES_PK}
akaikkr-aiida submit-followup --go-pk {GO_PK} --mode jij                                                             # -> pk {JIJ_PK}
akaikkr-aiida report --pk {GAES_PK} --jij-pk {JIJ_PK} --lang ja"""))
both("""<table><tr><th>他の言い方</th><th>Claude が呼ぶもの</th></tr>
<tr><td>「Cu の DOS とバンド分散を計算して」</td><td><code>kkr_submit_chain(preset="Cu", modes="dos,spc")</code></td></tr>
<tr><td>「今走っている計算は？」</td><td><code>kkr_list()</code>、<code>kkr_process(pk)</code></td></tr>
<tr><td>「pk 2682 の結果を見せて」</td><td><code>kkr_results(pk=2682)</code></td></tr>
<tr><td>「SeMnFeCo の Se 4s を core にして ewidth を決めて」</td><td><code>kkr_submit_gaes(comp="SeMnFeCo", polytyp="fcc", orbital="Se4s=core")</code></td></tr></table>
<div class="note">全部の操作は CLI <code>akaikkr-aiida</code> でも同じにできます（MCP は CLI を呼ぶ薄い層です）。行動記録は <code>~/.aiida-akaikkr/log/</code> に残ります。</div>""",
     """<table><tr><th>Other phrasings</th><th>What Claude calls</th></tr>
<tr><td>"compute the DOS and the band dispersion of Cu"</td><td><code>kkr_submit_chain(preset="Cu", modes="dos,spc")</code></td></tr>
<tr><td>"what is running now?"</td><td><code>kkr_list()</code>, <code>kkr_process(pk)</code></td></tr>
<tr><td>"show me the results of pk 2682"</td><td><code>kkr_results(pk=2682)</code></td></tr>
<tr><td>"decide ewidth for SeMnFeCo with Se 4s as core"</td><td><code>kkr_submit_gaes(comp="SeMnFeCo", polytyp="fcc", orbital="Se4s=core")</code></td></tr></table>
<div class="note">Everything can also be done with the CLI <code>akaikkr-aiida</code> (the MCP server is a thin layer that calls it). Actions are logged in <code>~/.aiida-akaikkr/log/</code>.</div>""")

# setup
h2("setup", "動かすまで", "Getting started")
shared(code("""# conda env akaikkr: aiida-core 2.9, RabbitMQ, pyakaikkr, aiida-akaikkr
pip install -e AkaiKKRPythonUtil/library/PyAkaiKKR    # pyakaikkr
pip install -e aiida-akaikkr                           # plugin (entry points akaikkr.*)
verdi presto --profile-name akaikkr                    # profile
verdi computer setup ... / verdi code create ...       # computer (SLURM, ssh) and the installed specx
verdi daemon start
akaikkr-aiida status                                   # check
claude mcp add akaikkr -- /path/to/env/bin/akaikkr-mcp --allow-submit   # register with Claude"""))

# notes
h2("notes", "知っておくこと", "Things to know")
both("""<ul>
<li>ewidth をわずかに変えるだけで SCF の収束の可否が反転することがあります。収束したことは積分路がギャップにある証拠にはならず、逆も同じです。判定は DOS で行います。</li>
<li>Hf は収束しない。AkaiKKR の reconf でエラーが起きる。</li>
<li>多原子胞ではギャップ判定の DOS を原子あたりに直します（既定）。1 原子の CPA 用に決めた閾値をそのまま使うためです。</li>
<li>いまの GAES は全体の DOS だけで判定します。濃度の小さい成分（例: La<sub>0.999</sub>Ge<sub>0.001</sub> の Ge）の semicore は全体の DOS には濃度分しか現れず、閾値と同じ程度なので見つけられません。
成分ごとの DOS（PDOS）を読んで判定に加える予定です（TODO）。</li></ul>""",
     """<ul>
<li>A tiny change of ewidth can flip whether the SCF converges. Convergence is no evidence that the contour lies in a gap, and vice versa; the judgement is made on the DOS.</li>
<li>Hf does not converge: AkaiKKR stops with an error in reconf.</li>
<li>For cells with several atoms the DOS used for the gap judgement is taken per atom (default), so that thresholds set for one-atom CPA cells carry over.</li>
<li>GAES currently judges on the total DOS only. The semicore of a dilute component (e.g. Ge in La<sub>0.999</sub>Ge<sub>0.001</sub>) appears in the total DOS only in proportion to its
concentration, comparable to the threshold, and is missed. Reading the DOS per component (PDOS) into the judgement is planned (TODO).</li></ul>""")

# appendix: the report
h2("appendix", "付録: SmCo5 のレポート（利点 4 の例の出力）", "Appendix: the SmCo5 report (output of the example of benefit 4)")
def report_body(path, tag):
    rep = open(path, encoding="utf-8").read()
    body = re.search(r"<body>(.*)</body>", rep, re.S).group(1)
    return body.replace('id="', 'id="rep-{}-'.format(tag)).replace('href="#', 'href="#rep-{}-'.format(tag))     # keep ids unique


both('<p class="cap">「pk 3799 のレポートを日本語で作って」（<code>kkr_report(pk=3799, jij_pk=3887, lang="ja")</code>）で得られたファイルをそのまま載せています。</p>',
     '<p class="cap">The file obtained with "make the report of pk 3799 in English" (<code>kkr_report(pk=3799, jij_pk=3887, lang="en")</code>) is embedded as is.</p>')
both('<div class="report">{}</div>'.format(report_body(REPORT_JA, "ja")), '<div class="report">{}</div>'.format(report_body(REPORT_EN, "en")))

both('<p class="meta">aiida-akaikkr v1.0.0 / pyakaikkr v1.0.0（2026-09-26）。図は inline SVG（来歴図は AiiDA の graphviz 出力、その他は pyakaikkr.plot）。</p>',
     '<p class="meta">aiida-akaikkr v1.0.0 / pyakaikkr v1.0.0 (2026-09-26). Figures are inline SVG (the provenance graph from AiiDA / graphviz, the rest from pyakaikkr.plot).</p>')

page = ['<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8"><title>aiida-akaikkr 入門 / Introduction</title>',
        "<style>{}</style><script>{}</script></head><body>".format(CSS, JS),
        '<div class="langbar"><button data-lang="ja" onclick="setLang(\'ja\')">日本語</button><button data-lang="en" onclick="setLang(\'en\')">English</button></div>']
page += S
page.append("</body></html>")
text = fill("\n".join(page))
open(OUT, "w", encoding="utf-8").write(text)
print("wrote", OUT, "%.0f KB" % (os.path.getsize(OUT) / 1024))
