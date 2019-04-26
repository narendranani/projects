import logging
import os

from flask import Flask
from flask_ask import Ask, request, session, question, statement, audio

app = Flask(__name__)
ask = Ask(app, "/")
logging.getLogger('flask_ask').setLevel(logging.DEBUG)

import pyaudio
import wave
import sys
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()
FORMAT = pyaudio.paInt16
CHANNELS = 2
RATE = 44100
CHUNK = 1024
RECORD_SECONDS = 3


def record(stream):
    LOGGER.info("recording...")
    frames = []

    for i in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
        data = stream.read(CHUNK)
        frames.append(data)
    LOGGER.info("finished recording")
    return frames


def play(stream, frames):
    """ Play the plain recorded stream. """
    dataout = b''.join(frames)
    for i in range(0, len(dataout), CHUNK):
        stream.write(dataout[i:i + CHUNK])


def stretch(snd_array, factor, window_size, h):
    """ Stretches/shortens a sound, by some factor. """
    phase = np.zeros(window_size)
    hanning_window = np.hanning(window_size)
    result = np.zeros(int(len(snd_array) / factor + window_size))
    for i in np.arange(0, len(snd_array) - (window_size + h), h * factor):
        i = int(i)
        # Two potentially overlapping subarrays
        a1 = snd_array[i: i + window_size]
        a2 = snd_array[i + h: i + window_size + h]

        # The spectra of these arrays
        s1 = np.fft.fft(hanning_window * a1)
        s2 = np.fft.fft(hanning_window * a2)

        # Rephase all frequencies
        phase = (phase + np.angle(s2 / s1)) % 2 * np.pi

        a2_rephased = np.fft.ifft(np.abs(s2) * np.exp(1j * phase))
        i2 = int(i / factor)
        result[i2: i2 + window_size] += hanning_window * a2_rephased.real
    return result.astype('int16')


def speedx(sound_array, factor):
    """ Multiplies the sound's speed by some `factor` """
    indices = np.round(np.arange(0, len(sound_array), factor))
    indices = indices[indices < len(sound_array)].astype(int)
    return sound_array[indices.astype(int)]


def pitchshift(snd_array, n, window_size=2 ** 13, h=2 ** 11):
    """ Changes the pitch of a sound by ``n`` semitones. """
    factor = 2 ** (1.0 * n / 12.0)
    stretched = stretch(snd_array, 1.0 / factor, window_size, h)
    return speedx(stretched[window_size:], factor)


def play_with_pitch_shift(stream, frames):
    """ Play recorded stream with shifted pitch"""
    dataout = b''.join(frames)
    data = np.fromstring(dataout, dtype=np.int16)
    data *= 4
    # Play audio
    pitched = pitchshift(data, 5)
    sound = (pitched.astype(np.int16).tostring())

    for i in range(0, len(sound), CHUNK):
        stream.write(sound[i:i + CHUNK])
    LOGGER.info("Finished playing.")


@ask.launch
def launch():
    speech_text = 'Hi! I am mini. You can talk to me.'
    return question(speech_text).reprompt(speech_text).simple_card('HelloWorld', speech_text)


@ask.intent('voiceRepeatIntent', mapping={'voice': 'voice'})
def voice_repeat(voice):
    import pyttsx3
    engine = pyttsx3.init()
    engine.setProperty('voice', 'HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\TTS_MS_EN-US_ZIRA_11.0')
    engine.say('Start')
    engine.runAndWait()
    paudio = pyaudio.PyAudio()
    stream = paudio.open(format=FORMAT, channels=CHANNELS,
                         rate=RATE, input=True, output=True,
                         frames_per_buffer=CHUNK)
    frames = record(stream)
    # play(stream, frames)
    play_with_pitch_shift(stream, frames)
    return question("")


@ask.intent('AMAZON.HelpIntent')
def help():
    speech_text = 'Talk to me anything. I will repeat your words'
    return question(speech_text).reprompt(speech_text).simple_card('HelloWorld', speech_text)


@ask.session_ended
def session_ended():
    return "{}", 200


@ask.intent('AMAZON.StopIntent')
def stop_skill():
    text = ["Bye Bye! Take care", ""]
    i = 0
    return statement(text[i])


if __name__ == '__main__':
    if 'ASK_VERIFY_REQUESTS' in os.environ:
        verify = str(os.environ.get('ASK_VERIFY_REQUESTS', '')).lower()
        if verify == 'false':
            app.config['ASK_VERIFY_REQUESTS'] = False
    app.run(debug=True)
