from flask import Flask
from datetime import datetime
from downloader import main
from multiprocessing import Process, Queue

app = Flask(__name__)


def acquire_data():
    p = Process(target=acquire_data_async)
    p.start()

def acquire_data_async():
    try:
        main()
    except Exception :
        print("Failed to acquire data")

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
    acquire_data_async()
    return """
    <h1>Hello heroku</h1>
    <h2>It is currently {time}.</h2>
    <h3>Started downloading data</h3>

    <img src="http://loremflickr.com/600/400">
    """.format(time=the_time)

if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)
