from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/process', methods=['POST'])
def process():
    data = request.json.get('data', [])
    if not data: return jsonify({"error": "No data"}), 400
    
    # عملية خاصة: تحليل إحصائي
    import statistics
    res = {
        "mean": statistics.mean(data),
        "median": statistics.median(data),
        "stdev": statistics.stdev(data) if len(data) > 1 else 0
    }
    return jsonify({"special_result": res, "stack": "Python/Flask"})

if __name__ == '__main__':
    app.run(port=5001)