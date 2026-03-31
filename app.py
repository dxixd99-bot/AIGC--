from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import pandas as pd
import uuid

app = Flask(__name__)
CORS(app)  # 跨域全开，解决下载问题

UPLOAD_FOLDER = "/tmp/uploads"
OUTPUT_FOLDER = "/tmp/output"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@app.route("/merge_excel", methods=["POST"])
def merge_excel():
    files = request.files.getlist("files[]")
    dedup_enable = request.form.get("dedup_enable") == "true"
    dedup_type = request.form.get("dedup_type")

    all_data = []
    for file in files:
        try:
            df = pd.read_excel(file)
            df["来源文件"] = file.filename
            all_data.append(df)
        except:
            pass

    combined = pd.concat(all_data, ignore_index=True)
    original = len(combined)
    dup_count = 0
    col = None

    if dedup_enable:
        cols = [c.upper() for c in combined.columns]
        if dedup_type == "skuid":
            for c in combined.columns:
                if "SKUID" in c.upper():
                    col = c
                    break
        elif dedup_type == "videourl":
            for c in combined.columns:
                if "URL" in c.upper() or "视频" in c.upper():
                    col = c
                    break
        if col:
            before = len(combined)
            combined = combined.drop_duplicates(subset=[col], keep="first")
            dup_count = before - len(combined)

    fn = f"merged_{uuid.uuid4().hex[:8]}.xlsx"
    fp = os.path.join(OUTPUT_FOLDER, fn)
    combined.to_excel(fp, index=False)

    return jsonify({
        "code": 200,
        "file": fn,
        "file_count": len(files),
        "original_rows": original,
        "duplicate_removed": dup_count,
        "final_rows": len(combined),
        "dedup_column": col
    })

@app.route("/download/<filename>")
def download(filename):
    p = os.path.join(OUTPUT_FOLDER, filename)
    return send_file(
        p,
        as_attachment=True,
        download_name="合并去重结果.xlsx"
    )

@app.route("/")
def home():
    return "后端运行中"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
