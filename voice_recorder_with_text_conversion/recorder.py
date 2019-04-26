import wx
import os
import wx.lib.buttons as buttons
# import recording_to_text as rt
import speech_recognition as sr
import _thread

dirName = os.path.dirname(os.path.abspath(__file__))
bitmapDir = os.path.join(dirName, 'bitmaps')


def recognize_speech_from_mic(recognizer, microphone):
    """Transcribe speech from recorded from `microphone`.

    Returns a dictionary with three keys:
    "success": a boolean indicating whether or not the API request was
               successful
    "error":   `None` if no error occured, otherwise a string containing
               an error message if the API could not be reached or
               speech was unrecognizable
    "transcription": `None` if speech could not be transcribed,
               otherwise a string containing the transcribed text
    """
    # check that recognizer and microphone arguments are appropriate type
    if not isinstance(recognizer, sr.Recognizer):
        raise TypeError("`recognizer` must be `Recognizer` instance")

    if not isinstance(microphone, sr.Microphone):
        raise TypeError("`microphone` must be `Microphone` instance")

    # adjust the recognizer sensitivity to ambient noise and record audio
    # from the microphone
    with microphone as source:
        recognizer.adjust_for_ambient_noise(source)
        audio = recognizer.listen(source)

    # set up the response object
    response = {
        "success": True,
        "error": None,
        "transcription": None
    }

    # try recognizing the speech in the recording
    # if a RequestError or UnknownValueError exception is caught,
    #     update the response object accordingly
    try:
        response["transcription"] = recognizer.recognize_google(audio)
    except sr.RequestError:
        # API was unreachable or unresponsive
        response["success"] = False
        response["error"] = "API unavailable"
    except sr.UnknownValueError:
        # speech was unintelligible
        response["error"] = "Unable to recognize speech"

    return response


class MainView(wx.Frame):
    # wx.Frame.ShowFullScreen()
    def __init__(self):
        """Main Application Window"""
        # wx.Frame.__init__(self, None, title="Data Generator")
        wx.Frame.__init__(self, None, title="Voice Recorder With Text Output",
                          style=wx.DEFAULT_FRAME_STYLE | wx.TAB_TRAVERSAL)
        # self.panel = wx.Panel(self, wx.ID_ANY)
        self.Maximize(True)
        self.panel = wx.Panel(self, wx.ID_ANY, style=wx.SUNKEN_BORDER | wx.TAB_TRAVERSAL)
        self.sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.view()

    def view(self):
        # self.btn_record = wx.Button(self.panel, wx.ID_ANY, "Record", wx.DefaultPosition, wx.Size(50,25), 10)
        # self.sizer.Add(self.btn_record, 2, wx.ALIGN_CENTER_VERTICAL | wx.ALIGN_CENTER_HORIZONTAL,  0)

        img = wx.Bitmap(os.path.join(bitmapDir, "mic_icon.gif"))
        self.btn_record = buttons.GenBitmapToggleButton(self.panel, bitmap=img, name="play")
        self.btn_record.Enable(True)
        self.btn_record.SetInitialSize()
        # self.btn_record.Bind(wx.EVT_BUTTON, self.record)
        gsizer_db_details = wx.GridSizer(rows=1, cols=1, hgap=0, vgap=5)
        gsizer_db_details.Add(self.btn_record, 1, wx.ALL | wx.ALIGN_CENTER | wx.SHAPED, 0)
        # self.sizer.Add(self.btn_record, 2, wx.ALIGN_CENTER_HORIZONTAL, 50)
        self.sizer.Add(gsizer_db_details, 2, wx.ALIGN_CENTER, 0)
        self.btn_record.Bind(wx.EVT_BUTTON, self.on_record_button)
        self.txt_box = wx.TextCtrl(self.panel, wx.ID_ANY, "", size=wx.DefaultSize,
                                   style=wx.TE_MULTILINE | wx.TE_READONLY)
        self.sizer.Add(self.txt_box, 1, wx.EXPAND | wx.ALIGN_CENTER, 0)
        self.sizer.Layout()
        self.sizer.Fit(self.panel)
        self.panel.SetSizer(self.sizer)
        self.panel.Layout()
        self.Layout()

    def on_record_button(self, event=None):
        if self.btn_record.GetValue():
            _thread.start_new_thread(self.record, ())
        else:
            print("Stopped recording")
            return

    def record(self):
        recognizer = sr.Recognizer()
        microphone = sr.Microphone()
        while True:
            # for i in range(1, 5):
            response = recognize_speech_from_mic(recognizer, microphone)
            print("response: ", response["transcription"])
            text_string = str(response["transcription"]) if response["transcription"] else ''
            self.txt_box.AppendText('\n' + text_string)
            if not self.btn_record.GetValue():
                break


if __name__ == "__main__":
    app = wx.App(False)
    frame = MainView()
    frame.Show()
    app.MainLoop()
