from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import pandas as pd
import uuid

app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
    return "✅ 后端运行成功"

@app.route("/merge_excel", methods=["POST"])
def merge():
    try:
        files = request.files.getlist("files[]")
        dedup_enable = request.form.get("dedup_enable") == "true"
        dedup_type = request.form.get("dedup_type")

        dfs = []
        for f in files:
            try:
                df = pd.read_excel(f)
                df["来源文件"] = f.filename
                dfs.append(df)
            except:
                pass

        total = pd.concat(dfs, ignore_index=True)
        ori = len(total)
        dup = 0
        col = None

        if dedup_enable:
            if dedup_type == "skuid":
                for c in total.columns:
                    if "SKUID" in c.upper():
                        col = c
                        break
            elif dedup_type == "videourl":
                for c in total.columns:
                    if "URL" in c.upper() or "视频" in c.upper():
                        col = c
                        break
            if col:
                before = len(total)
                total = total.drop_duplicates(subset=[col], keep="first")
                dup = before - len(total)

        fn = f"merged_{uuid.uuid4().hex[:8]}.xlsx"
        total.to_excel(f"/tmp/{fn}", index=False)

        return jsonify({
            "code": 200,
            "file": fn,
            "file_count": len(files),
            "original_rows": ori,
            "duplicate_removed": dup,
            "final_rows": len(total),
            "dedup_column": col
        })
    except Exception as e:
        return jsonify({"code": 500, "msg": str(e)})

@app.route("/download/<filename>")
def dl(filename):
    return send_file(f"/tmp/{filename}", as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
