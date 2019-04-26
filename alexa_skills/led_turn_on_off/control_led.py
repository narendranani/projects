import logging
import os

from flask import Flask
from flask_ask import Ask, request, session, question, statement, audio

app = Flask(__name__)
ask = Ask(app, "/")
logging.getLogger('flask_ask').setLevel(logging.DEBUG)




@ask.launch
def launch():
    speech_text = 'Welcome to LED controller! You can turn on the LED by saying turn on and turn off the led by saying turn off'
    return question(speech_text).reprompt(speech_text).simple_card('Control LED', speech_text)


@ask.intent('controlLEDIntent', mapping={'turn': 'turn'})
def control_led(turn):
    import RPi.GPIO as GPIO
    import time
    GPIO.setmode(GPIO.BCM)
    GPIO.setwarnings(False)
    GPIO.setup(18,GPIO.OUT)
    if str(turn) == 'on':
        speech = 'Ok. Turning on the LED'
        GPIO.output(18,GPIO.HIGH)
    elif str(turn) == 'off':
        speech = 'Ok. Turning off the LED'
        GPIO.output(18,GPIO.LOW)
    else:
        speech = "Sorry. I didn't get you. Please say 'Turn On' to turn the LED and 'Turn off' to turn off the led"
    return question(speech).reprompt(speech)


@ask.intent('AMAZON.HelpIntent')
def help():
    speech_text = 'This is LED controller skill. You can turn on the LED by saying turn on and turn off the led by saying turn off'
    return question(speech_text).reprompt(speech_text).simple_card('HelloWorld', speech_text)


@ask.session_ended
def session_ended():
    return "{}", 200


if __name__ == '__main__':
    if 'ASK_VERIFY_REQUESTS' in os.environ:
        verify = str(os.environ.get('ASK_VERIFY_REQUESTS', '')).lower()
        if verify == 'false':
            app.config['ASK_VERIFY_REQUESTS'] = False
    app.run(debug=True)
