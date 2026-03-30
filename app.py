from flask import Flask, request, jsonify, send_file
import os
import pandas as pd
import uuid
from werkzeug.utils import secure_filename

app = Flask(__name__)

# 线上环境自动创建目录
UPLOAD_FOLDER = "/tmp/uploads"
OUTPUT_FOLDER = "/tmp/output"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {"xlsx", "xls"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

# ========================
# 多表合并 + 去重（线上接口）
# ========================
@app.route("/merge_excel", methods=["POST"])
def merge_excel():
    try:
        files = request.files.getlist("files[]")
        dedup_enable = request.form.get("dedup_enable") == "true"
        dedup_type = request.form.get("dedup_type", "skuid")

        if not files:
            return jsonify({"code": 400, "msg": "未上传文件"}), 400

        all_data = []
        success_files = []

        for file in files:
            if file and allowed_file(file.filename) and not file.filename.startswith("~$"):
                filename = secure_filename(file.filename)
                save_path = os.path.join(UPLOAD_FOLDER, filename)
                file.save(save_path)

                try:
                    if filename.endswith(".xlsx"):
                        df = pd.read_excel(save_path, engine="openpyxl")
                    else:
                        df = pd.read_excel(save_path, engine="xlrd")

                    df["来源文件"] = filename
                    all_data.append(df)
                    success_files.append(filename)
                except:
                    pass

        if not all_data:
            return jsonify({"code": 400, "msg": "无有效Excel文件"}), 400

        # 合并
        combined_df = pd.concat(all_data, ignore_index=True)
        original_rows = len(combined_df)
        duplicate_count = 0
        target_col = None

        # 去重（勾选才执行）
        if dedup_enable:
            cols_upper = [str(c).strip().upper() for c in combined_df.columns]

            if dedup_type == "skuid":
                for i, c in enumerate(combined_df.columns):
                    if cols_upper[i] == "SKUID":
                        target_col = c
                        break

            elif dedup_type == "videourl":
                for i, c in enumerate(combined_df.columns):
                    if "VIDEO" in cols_upper[i] or "URL" in cols_upper[i] or "视频" in cols_upper[i]:
                        target_col = c
                        break

            if target_col:
                before = len(combined_df)
                combined_df = combined_df.drop_duplicates(subset=[target_col], keep="first")
                duplicate_count = before - len(combined_df)

        # 保存结果
        final_file = f"merged_{uuid.uuid4().hex[:8]}.xlsx"
        final_path = os.path.join(OUTPUT_FOLDER, final_file)
        combined_df.to_excel(final_path, index=False, engine="openpyxl")

        return jsonify({
            "code": 200,
            "msg": "合并成功",
            "file": final_file,
            "file_count": len(success_files),
            "original_rows": original_rows,
            "final_rows": len(combined_df),
            "duplicate_removed": duplicate_count,
            "dedup_column": target_col if dedup_enable else "未去重"
        })

    except Exception as e:
        return jsonify({"code": 500, "msg": str(e)}), 500

# 下载
@app.route("/download/<filename>")
def download(filename):
    path = os.path.join(OUTPUT_FOLDER, filename)
    return send_file(path, as_attachment=True)

# 健康检查（线上必须）
@app.route("/")
def index():
    return "AIGC 数据处理服务运行中 ✅"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=True)
