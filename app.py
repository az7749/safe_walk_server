from flask import Flask

app = Flask(__name__)

@app.route("/")
def home():
    return "서버 실행 중"

if __name__ == "__main__":
    app.run(debug=True)