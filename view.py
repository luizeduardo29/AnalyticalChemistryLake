from __future__ import annotations

from flask import Flask, request, jsonify, render_template_string
import clickhouse_connect


CH = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="user",
    password="Default@2026",
    database="analyticalChemistryLake",
)

app = Flask(__name__)


HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Dynamic Visualization</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background:#fff; }
    .title { font-size:18px; font-weight:700; margin-bottom:6px; display:flex; gap:10px; align-items:center; }
    .subtitle { color:#666; font-size:12px; line-height:1.35; margin-bottom:12px; }
    .divider { border-top:1px solid #eee; margin:14px 0; }

    .section { margin-bottom:14px; }
    .section h3 { font-size:14px; margin:0 0 10px 0; display:flex; gap:8px; align-items:center; }
    .icon { font-size:14px; }

    label { display:block; font-size:12px; font-weight:700; margin:10px 0 6px; }
    input, select { width: 340px; max-width: 90vw; padding:10px; border:1px solid #ddd; border-radius:8px; font-size:13px; }
    .btn { margin-top:12px; padding:10px 14px; border-radius:8px; border:0; background:#e6e6e6; font-weight:800; letter-spacing:1px; cursor:pointer; }
    .btn:disabled { opacity:0.5; cursor:not-allowed; }
    .hint { color:#777; font-size:12px; margin-top:6px; }

    .wrap { display:flex; gap:18px; flex-wrap:wrap; align-items:flex-start; }
    .panel { width: 420px; max-width: 95vw; border:1px solid #eee; border-radius:12px; padding:14px; }
    .plot { flex: 1; min-width: 360px; border:1px solid #eee; border-radius:12px; padding:14px; }
    #plot_area { width:100%; height:650px; }
  </style>
</head>
<body>

<div class="wrap">
  <div class="panel">
    <div class="title">📈 Dynamic Visualization</div>
    <div class="subtitle">
      <div><b>Origin:</b> Brazilian Doping Control Laboratory</div>
      <div class="hint">Selecione técnica → amostra → canal, depois busque.</div>
    </div>

    <label>Técnica</label>
    <select id="technique">
      <option value="" selected disabled>Select</option>
      <option value="GC-MS">GC-MS/MS</option>
      <option value="LC-MS">LC-MS/MS</option>
    </select>

    <div class="divider"></div>

    <div id="gcms_block" style="display:none;">
      <div class="section">
        <h3><span class="icon">🎯</span> Fragments</h3>

        <label>Samples</label>
        <select id="gcms_sample"></select>

        <label>SIM's</label>
        <select id="gcms_channel"></select>

        <button class="btn" id="gcms_btn">SEARCH CHROMATOGRAM(S)</button>
      </div>
    </div>

    <div id="lcms_block" style="display:none;">
      <div class="section">
        <h3><span class="icon">🎯</span> LC-MS</h3>

        <label>Samples</label>
        <select id="lcms_sample"></select>

        <label>SIM's / Scan Filters</label>
        <select id="lcms_channel"></select>

        <div class="hint">
          [CHROM] vem de <code>chromatogram_points</code><br>
          [SCAN] gera TIC (MS1/MS2) de <code>lcms_scans + lcms_spectra_points</code>
        </div>

        <button class="btn" id="lcms_btn">SEARCH CHROMATOGRAM(S)</button>
      </div>
    </div>
  </div>

  <div class="plot">
    <div id="plot_area"></div>
    <div id="plot_msg" class="hint">Selecione técnica → amostra → canal e clique em buscar.</div>
  </div>
</div>

<script>
  const techniqueEl = document.getElementById("technique");
  const gcmsBlock = document.getElementById("gcms_block");
  const lcmsBlock = document.getElementById("lcms_block");

  const plotArea = document.getElementById("plot_area");
  const plotMsg = document.getElementById("plot_msg");

  const gcmsSample = document.getElementById("gcms_sample");
  const gcmsChannel = document.getElementById("gcms_channel");
  const gcmsBtn = document.getElementById("gcms_btn");

  const lcmsSample = document.getElementById("lcms_sample");
  const lcmsChannel = document.getElementById("lcms_channel");
  const lcmsBtn = document.getElementById("lcms_btn");

  function setSelectLoading(sel, text="Loading...") {
    sel.innerHTML = "";
    const opt = document.createElement("option");
    opt.value = "";
    opt.disabled = true;
    opt.selected = true;
    opt.textContent = text;
    sel.appendChild(opt);
  }

  function setSelectEmpty(sel, text="Select") {
    sel.innerHTML = "";
    const opt = document.createElement("option");
    opt.value = "";
    opt.disabled = true;
    opt.selected = true;
    opt.textContent = text;
    sel.appendChild(opt);
  }

  async function loadSamplesForTechnique(tech, targetSelect) {
    setSelectLoading(targetSelect);
    const r = await fetch(`/api/samples?technique=${encodeURIComponent(tech)}`);
    const data = await r.json();
    setSelectEmpty(targetSelect, "Select");
    for (const s of data.samples) {
      const opt = document.createElement("option");
      opt.value = s.sample_id;
      opt.textContent = `${s.sample_name} (${s.sample_id})`;
      targetSelect.appendChild(opt);
    }
  }

  async function loadChannels(tech, sampleId, targetSelect) {
    setSelectLoading(targetSelect);
    const r = await fetch(`/api/channels?technique=${encodeURIComponent(tech)}&sample_id=${encodeURIComponent(sampleId)}`);
    const data = await r.json();
    setSelectEmpty(targetSelect, "Select");
    for (const c of data.channels) {
      const opt = document.createElement("option");
      opt.value = c.channel_id;
      opt.textContent = c.label;
      opt.dataset.kind = c.kind; // chrom|scan
      targetSelect.appendChild(opt);
    }
  }

  function showMsg(msg) {
    plotMsg.textContent = msg;
  }

  function renderPlot(payload) {
    Plotly.newPlot(plotArea, payload.traces, payload.layout, {responsive:true});
  }

  async function plot(url) {
    showMsg("Loading plot...");
    const r = await fetch(url);
    const data = await r.json();
    if (!data.ok) {
      showMsg("Erro: " + data.error);
      Plotly.purge(plotArea);
      return;
    }
    showMsg("");
    renderPlot(data);
  }

  techniqueEl.addEventListener("change", async () => {
    const tech = techniqueEl.value;

    Plotly.purge(plotArea);
    showMsg("Selecione técnica → amostra → canal e clique em buscar.");

    gcmsBlock.style.display = (tech === "GC-MS") ? "block" : "none";
    lcmsBlock.style.display = (tech === "LC-MS") ? "block" : "none";

    if (tech === "GC-MS") {
      await loadSamplesForTechnique("GC-MS", gcmsSample);
      setSelectEmpty(gcmsChannel, "Select");
    }
    if (tech === "LC-MS") {
      await loadSamplesForTechnique("LC-MS", lcmsSample);
      setSelectEmpty(lcmsChannel, "Select");
    }
  });

  gcmsSample.addEventListener("change", async () => {
    await loadChannels("GC-MS", gcmsSample.value, gcmsChannel);
  });

  lcmsSample.addEventListener("change", async () => {
    await loadChannels("LC-MS", lcmsSample.value, lcmsChannel);
  });

  gcmsBtn.addEventListener("click", async () => {
    if (!gcmsSample.value || !gcmsChannel.value) return;
    await plot(`/api/plot/chrom?sample_id=${encodeURIComponent(gcmsSample.value)}&channel_id=${encodeURIComponent(gcmsChannel.value)}&technique=GC-MS`);
  });

  lcmsBtn.addEventListener("click", async () => {
    if (!lcmsSample.value || !lcmsChannel.value) return;
    const selected = lcmsChannel.options[lcmsChannel.selectedIndex];
    const kind = selected.dataset.kind;

    if (kind === "scan") {
      await plot(`/api/plot/tic?sample_id=${encodeURIComponent(lcmsSample.value)}&channel_id=${encodeURIComponent(lcmsChannel.value)}`);
    } else {
      await plot(`/api/plot/chrom?sample_id=${encodeURIComponent(lcmsSample.value)}&channel_id=${encodeURIComponent(lcmsChannel.value)}&technique=LC-MS`);
    }
  });
</script>

</body>
</html>
"""


def list_samples_by_technique(technique: str, limit: int = 2000):
    rows = CH.query(
        f"""
        SELECT DISTINCT s.sample_id, s.sample_name
        FROM analyticalChemistryLake.samples s
        INNER JOIN analyticalChemistryLake.sample_channels c
          ON c.sample_id = s.sample_id
        WHERE c.chromatography_technique = {{tech:String}}
        ORDER BY s.created_at DESC
        LIMIT {int(limit)}
        """,
        parameters={"tech": technique},
    ).result_rows
    return [{"sample_id": r[0], "sample_name": r[1]} for r in rows]


def list_channels_lcms_both(sample_id: str):
    rows = CH.query(
        """
        SELECT channel_id, scan_filter, sim_ion_name
        FROM analyticalChemistryLake.sample_channels
        WHERE sample_id = {sid:UUID}
          AND chromatography_technique = 'LC-MS'
          AND (scan_filter IS NOT NULL OR sim_ion_name IS NOT NULL)
        """,
        parameters={"sid": sample_id},
    ).result_rows

    out = []
    for channel_id, scan_filter, sim_ion_name in rows:
        if scan_filter:
            out.append({"channel_id": channel_id, "label": f"[SCAN] {scan_filter}", "kind": "scan"})
        elif sim_ion_name:
            out.append({"channel_id": channel_id, "label": f"[CHROM] {sim_ion_name}", "kind": "chrom"})
    out.sort(key=lambda x: x["label"])
    return out


def list_channels_gcms(sample_id: str):
    rows = CH.query(
        """
        SELECT channel_id, sim_ion_name
        FROM analyticalChemistryLake.sample_channels
        WHERE sample_id = {sid:UUID}
          AND chromatography_technique = 'GC-MS'
          AND sim_ion_name IS NOT NULL
          AND scan_filter IS NULL
        ORDER BY sim_ion_name
        """,
        parameters={"sid": sample_id},
    ).result_rows
    return [{"channel_id": r[0], "label": r[1], "kind": "chrom"} for r in rows]


def get_sample(sample_id: str):
    rows = CH.query(
        """
        SELECT sample_id, sample_name
        FROM analyticalChemistryLake.samples
        WHERE sample_id = {sid:UUID}
        LIMIT 1
        """,
        parameters={"sid": sample_id},
    ).result_rows
    return {"sample_id": rows[0][0], "sample_name": rows[0][1]} if rows else None


def fetch_chrom_points(channel_id: str):
    return CH.query(
        """
        SELECT rt, intensity
        FROM analyticalChemistryLake.chromatogram_points
        WHERE channel_id = {cid:UUID}
        ORDER BY rt
        """,
        parameters={"cid": channel_id},
    ).result_rows


def fetch_lcms_tic(channel_id: str):
    rows = CH.query(
        """
        SELECT s.ms_level, s.rt, p.tic
        FROM
        (
            SELECT channel_id, scan_index, sum(intensity) AS tic
            FROM analyticalChemistryLake.lcms_spectra_points
            WHERE channel_id = {cid:UUID}
            GROUP BY channel_id, scan_index
        ) p
        INNER JOIN
        (
            SELECT channel_id, scan_index, rt, ms_level
            FROM analyticalChemistryLake.lcms_scans
            WHERE channel_id = {cid:UUID}
        ) s
        ON p.channel_id = s.channel_id AND p.scan_index = s.scan_index
        ORDER BY s.ms_level, s.rt
        """,
        parameters={"cid": channel_id},
    ).result_rows

    ms1 = [(r[1], r[2]) for r in rows if r[0] == 1]
    ms2 = [(r[1], r[2]) for r in rows if r[0] == 2]
    return ms1, ms2


@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/samples")
def api_samples():
    tech = request.args.get("technique")
    if tech not in ("LC-MS", "GC-MS"):
        return jsonify({"samples": []})
    return jsonify({"samples": list_samples_by_technique(tech)})


@app.route("/api/channels")
def api_channels():
    tech = request.args.get("technique")
    sample_id = request.args.get("sample_id")

    if tech not in ("LC-MS", "GC-MS") or not sample_id:
        return jsonify({"channels": []})

    if tech == "LC-MS":
        return jsonify({"channels": list_channels_lcms_both(sample_id)})

    return jsonify({"channels": list_channels_gcms(sample_id)})


@app.route("/api/plot/chrom")
def api_plot_chrom():
    tech = request.args.get("technique")
    sample_id = request.args.get("sample_id")
    channel_id = request.args.get("channel_id")

    if tech not in ("LC-MS", "GC-MS"):
        return jsonify({"ok": False, "error": "Técnica inválida."})
    if not sample_id or not channel_id:
        return jsonify({"ok": False, "error": "Informe sample_id e channel_id."})

    sample = get_sample(sample_id)
    if not sample:
        return jsonify({"ok": False, "error": "Amostra não encontrada."})

    pts = fetch_chrom_points(channel_id)
    if not pts:
        return jsonify({"ok": False, "error": "Sem pontos em chromatogram_points para esse canal."})

    x = [p[0] for p in pts]
    y = [p[1] for p in pts]

    return jsonify({
        "ok": True,
        "traces": [{
            "type": "scatter",
            "mode": "lines",
            "name": "Chromatogram",
            "x": x,
            "y": y,
        }],
        "layout": {
            "title": f"{tech} — {sample['sample_name']}",
            "xaxis": {"title": "Retention Time (rt)"},
            "yaxis": {"title": "Intensity"},
            "height": 650,
            "margin": {"l": 50, "r": 20, "t": 50, "b": 40},
        }
    })


@app.route("/api/plot/tic")
def api_plot_tic():
    sample_id = request.args.get("sample_id")
    channel_id = request.args.get("channel_id")

    if not sample_id or not channel_id:
        return jsonify({"ok": False, "error": "Informe sample_id e channel_id."})

    sample = get_sample(sample_id)
    if not sample:
        return jsonify({"ok": False, "error": "Amostra não encontrada."})

    ms1, ms2 = fetch_lcms_tic(channel_id)
    if not ms1 and not ms2:
        return jsonify({"ok": False, "error": "Sem dados para gerar TIC (MS1/MS2) nesse scan_filter."})

    traces = []
    if ms1:
        traces.append({
            "type": "scatter",
            "mode": "lines",
            "name": "TIC (MS1)",
            "x": [r[0] for r in ms1],
            "y": [r[1] for r in ms1],
        })
    if ms2:
        traces.append({
            "type": "scatter",
            "mode": "lines",
            "name": "TIC (MS2)",
            "x": [r[0] for r in ms2],
            "y": [r[1] for r in ms2],
        })

    return jsonify({
        "ok": True,
        "traces": traces,
        "layout": {
            "title": f"LC-MS — TIC por scan_filter — {sample['sample_name']}",
            "xaxis": {"title": "Retention Time (rt)"},
            "yaxis": {"title": "TIC (sum intensity)"},
            "height": 650,
            "margin": {"l": 50, "r": 20, "t": 50, "b": 40},
        }
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
