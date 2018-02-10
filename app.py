from flask import Flask
from datetime import datetime
app = Flask(__name__)

@app.route('/')
def homepage():
    the_time = datetime.now().strftime("%A, %d. %B %Y %I:%M%p")

    return """
    <h1>Hello heroku</h1>
    <p>It is currently {time}.</p>

    <img src="http://loremflickr.com/600/400">
    """.format(time=the_time)


@app.route('/download')
def download():
    the_time = datetime.now().strftime("%A, %d. %B %Y %I:%M%p")

    return """
    <h1>Hello heroku</h1>
    <h2>It is currently {time}.</h2>
    <h3>I am downloading data</h3>

    <img src="http://loremflickr.com/600/400">
    """.format(time=the_time)

if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)
